/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

package com.teragrep.pth06.planner;

import com.teragrep.pth06.Config;
import com.teragrep.pth06.planner.walker.ConditionWalker;
import org.jooq.*;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * <h2>Archive Query Processor</h2>
 *
 * Class for controlled execution of Archive query offsets.
 *
 * @since 08/04/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 * @author Motoko Kusanagi
 */
public class ArchiveQueryProcessor implements ArchiveQuery {

    // NOTE, start is exclusive (GreaterThan), end is inclusive

    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveQueryProcessor.class);

    private final StreamDBClient sdc;
    // query conditions for stream and journal databases
    private Condition streamCondition;
    private Condition journalCondition;

    private final LocalDate startDay = LocalDate.ofYearDay(2010, 1); // FIXME, parametrize start range
    private final LocalDate endDay = LocalDate.now(); // FIXME, parametrize end range
    private LocalDate rollingDay = startDay;

    public ArchiveQueryProcessor(Config config) {

        System.out.println("ArchiveQueryProcessor Incoming: q=" + config.getQuery());
        if (config.getQuery() != null) {
            try {
                ConditionWalker parser = new ConditionWalker();
                // Construct both streamDB and journalDB query conditions
                streamCondition = parser.fromString(config.getQuery(), true);
                journalCondition = parser.fromString(config.getQuery(), false);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException("ArchiveQueryProcessor problems when construction Query conditions query:" + config.getQuery() + " exception:" + ex);
            }
        }

        try {
            this.sdc = new StreamDBClient(
                    config.getDBuserName(),
                    config.getDBpassword(),
                    config.getDBurl(),
                    config.getDBjournaldbname(),
                    config.getDBstreamdbname(),
                    streamCondition,
                    config.getHideDatabaseExceptions()
            );
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("StreamDB not connected.");
        }

        sdc.setIncludeBeforeEpoch(config.getArchiveIncludeBeforeEpoch());
    }

    private void seekToResults() {
        LOGGER.debug("ArchiveQueryProcessor.seekToResults>");

        int rows = sdc.pullToSliceTable(journalCondition, Date.valueOf(rollingDay));
        while (rows == 0 && rollingDay.isBefore(endDay)) {
            LOGGER.debug("ArchiveQueryProcessor.seekToResults> day " + rollingDay);
            rollingDay = rollingDay.plusDays(1);
            rows = sdc.pullToSliceTable(journalCondition, Date.valueOf(rollingDay));
        }
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processInitialUnixEpochHour() {
        LOGGER.debug("ArchiveQueryProcessor.processInitialUnixEpochHour>");
        seekToResults();
        Result<Record1<ULong>> hourSet = sdc.getEarliestHourFromSliceTable();

        long earliestHour = Long.MAX_VALUE; // will return nothing if not available

        if (hourSet.size() > 0 ) {
            earliestHour = hourSet.get(0).get(0, ULong.class).longValue();
            LOGGER.debug("ArchiveQueryProcessor.processInitialUnixEpochHour> get hour " + earliestHour);
        }

        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> result = sdc.getHour(earliestHour);
        LOGGER.debug("ArchiveQueryProcessor.processInitialUnixEpochHour> return " + result.formatCSV());

        return result;
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processAfterUnixEpochHour(long lastHour) {
        LOGGER.debug("ArchiveQueryProcessor.processAfterUnixEpochHour>");

        Result<Record1<ULong>> hourSet = sdc.getNextHourFromSliceTable(lastHour);

        if (hourSet.size() == 0) {
            rollingDay = rollingDay.plusDays(1);
            seekToResults();
        }

        hourSet = sdc.getNextHourFromSliceTable(lastHour);

        long nextHour = Long.MAX_VALUE; // will return nothing if not available

        if (hourSet.size() > 0 ) {
            nextHour = hourSet.get(0).get(0, ULong.class).longValue();
        }

        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> result = sdc.getHour(nextHour);
        LOGGER.debug("ArchiveQueryProcessor.processAfterUnixEpochHour> return args lastHour " + lastHour + " " + result.formatCSV());

        return result;
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processBeforeUnixEpochHour(long lastHour) {
        LOGGER.debug("ArchiveQueryProcessor.processBeforeUnixEpochHour>");
        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> result = sdc.getHoursBeforeFromSliceTable(lastHour);
        LOGGER.debug("ArchiveQueryProcessor.processBeforeUnixEpochHour> return args lastHour  " + lastHour + " " + result.formatCSV());

        return result;
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processBetweenUnixEpochHours(long startHour, long endHour) {
        LOGGER.debug("ArchiveQueryProcessor.processBetweenUnixEpochHours>");

        // a retry, so evreything must be within the temporaryTable as commit only cleans it
        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> result = sdc.getHourRange(startHour, endHour);
        LOGGER.debug("ArchiveQueryProcessor.processBetweenUnixEpochHours> return args startHour " + startHour +  " endHour " + endHour + " " + result.formatCSV());

        return result;
    }

    @Override
    public void commit(long offset) {
        LOGGER.debug("ArchiveQueryProcessor.commit>");
        sdc.deleteRangeFromSliceTable(Long.MIN_VALUE, offset);
    }
}
