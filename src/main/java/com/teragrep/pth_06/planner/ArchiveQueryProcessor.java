/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.walker.EarliestWalker;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.jooq.*;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * <h1>Archive Query Processor</h1> Class for controlled execution of Archive query offsets.
 *
 * @since 08/04/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 * @author Motoko Kusanagi
 * @author Ville Manninen
 */
public class ArchiveQueryProcessor implements ArchiveQuery {
    // NOTE: start is exclusive (GreaterThan), end is inclusive

    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveQueryProcessor.class);
    private final StreamDBClient sdc;
    private final LocalDate endDay = LocalDate.now(); // FIXME, parametrize end range
    private LocalDate rollingDay;
    private Long latestOffset = null;
    private final Long earliestEpoch;
    private final long quantumLength;
    private final long numPartitions;
    private final float compressionRatio;
    private final float processingSpeed;
    private final long totalObjectCountLimit;

    public ArchiveQueryProcessor(Config config) {
        // get configs from config object
        this.quantumLength = config.batchConfig.quantumLength;
        this.numPartitions = config.batchConfig.numPartitions;
        this.compressionRatio = config.batchConfig.fileCompressionRatio;
        this.processingSpeed = config.batchConfig.processingSpeed;
        this.totalObjectCountLimit = config.batchConfig.totalObjectCountLimit;

        System.out.println("ArchiveQueryProcessor Incoming: q=" + config.query);
        if (config.query == null) {
            throw new IllegalArgumentException("Query was not provided");
        }

        try {
            EarliestWalker earliestWalker = new EarliestWalker();
            this.earliestEpoch = earliestWalker.fromString(config.query);
            // TODO hack to update startDay from query
            rollingDay = Instant.ofEpochSecond(this.earliestEpoch).atZone(ZoneId.systemDefault()).toLocalDate();

        }
        catch (ParserConfigurationException | IOException | SAXException ex) {
            throw new RuntimeException(
                    "ArchiveQueryProcessor problems when construction Query conditions query:" + config.query
                            + " exception:" + ex
            );
        }

        try {
            this.sdc = new StreamDBClient(config);
        }
        catch (SQLException e) {
            throw new RuntimeException("StreamDB not connected.");
        }
    }

    /**
     * Increment the rolling day and pull data for that day into the sliceTable. Continued until endDay is reached or
     * sliceTable has data for current day.
     */
    private void seekToResults() {
        LOGGER.debug("ArchiveQueryProcessor.seekToResults>");
        int rows = sdc.pullToSliceTable(Date.valueOf(rollingDay));

        LOGGER.debug("Got {} row(s) on {}", rows, rollingDay);
        while (rows == 0 && rollingDay.isBefore(endDay)) {
            rollingDay = rollingDay.plusDays(1);
            rows = sdc.pullToSliceTable(Date.valueOf(rollingDay));
            LOGGER.debug("Got {} row(s) on {}", rows, rollingDay);
        }
    }

    /**
     * Get data from the SliceTable between startHour and endHour.
     *
     * @param startHour Exclusive start hour
     * @param endHour   Inclusive end hour
     * @return Data between start hour and end hour.
     */
    @Override
    public Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> processBetweenUnixEpochHours(
            long startHour,
            long endHour
    ) {
        LOGGER.debug("ArchiveQueryProcessor.processBetweenUnixEpochHours> [{}, {}[", startHour, endHour);

        // a retry, so everything must be within the temporaryTable as commit only cleans it
        return sdc.getHourRange(startHour, endHour);
    }

    /**
     * Deletes the contents of the sliceTable up until commit offset.
     * 
     * @param offset Last committed offset
     */
    @Override
    public void commit(long offset) {
        LOGGER.debug("ArchiveQueryProcessor.commit>");
        sdc.deleteRangeFromSliceTable(Long.MIN_VALUE, offset);
    }

    /**
     * Returns the earliest offset, which always remains the same within a query.
     * 
     * @return Earliest offset
     */
    @Override
    public Long getInitialOffset() {
        return this.earliestEpoch;
    }

    /**
     * Increments the latest offset value and returns that incremented offset. Works by pulling data into the SliceTable
     * until weight limit or endDay is reached.
     * 
     * @return latest offset
     */
    @Override
    public Long incrementAndGetLatestOffset() {
        // Get initial offset as latest offset in the beginning
        // and load that day's events into sliceTable
        if (this.latestOffset == null) {
            this.latestOffset = getInitialOffset();
            sdc.pullToSliceTable(Date.valueOf(rollingDay));
        }

        // Initialize the batchSizeLimit object to split the data into appropriate sized batches
        BatchSizeLimit batchSizeLimit = new BatchSizeLimit(quantumLength * numPartitions, totalObjectCountLimit);

        // Keep loading more offsets until the limit is reached
        while (!batchSizeLimit.isOverLimit()) {
            WeightedOffset weightedOffsetOfNextHour = sdc.getNextHourAndSizeFromSliceTable(this.latestOffset);

            // SliceTable was empty on latestOffset day
            if (weightedOffsetOfNextHour.isStub) {
                LOGGER.debug("Weighted offset was empty on day {}", rollingDay);

                // Stop loop on endDay: no future date processing
                if (rollingDay.isEqual(endDay) || rollingDay.isAfter(endDay)) {
                    break;
                }

                // Pull next day to sliceTable
                rollingDay = rollingDay.plusDays(1);
                seekToResults();
                weightedOffsetOfNextHour = sdc.getNextHourAndSizeFromSliceTable(this.latestOffset);
            }

            // If SliceTable was not empty on the processed day, estimate the weight of it to calculate
            // the limit and update the latestOffset.
            if (!weightedOffsetOfNextHour.isStub) {
                batchSizeLimit.add(weightedOffsetOfNextHour.estimateWeight(compressionRatio, processingSpeed));
                this.latestOffset = weightedOffsetOfNextHour.offset();
            }
        }

        LOGGER.info("Return latest offset: {}", this.latestOffset);
        return this.latestOffset;
    }

    @Override
    public Long mostRecentOffset() {
        return latestOffset;
    }

    @Override
    public CustomTaskMetric[] currentDatabaseMetrics() {
        return sdc.currentDatabaseMetrics();
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
