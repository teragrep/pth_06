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

import com.teragrep.pth_06.ast.analyze.ScanPlan;
import com.teragrep.pth_06.ast.analyze.ScanPlanCollection;
import com.teragrep.pth_06.ast.analyze.ScanPlanView;
import com.teragrep.pth_06.ast.analyze.View;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.source.HBaseSource;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public final class HBaseQueryImpl implements HBaseQuery {

    private final Logger LOGGER = LoggerFactory.getLogger(HBaseQueryImpl.class);

    private final Config config;
    private final ScanPlanCollection scanPlanCollection;
    private final LogfileTable table;
    private long latestCommited = Long.MIN_VALUE;
    private HourlyWindows hourlyWindows;

    public HBaseQueryImpl(final Config config, final HBaseSource source) {
        this(config, new ScanPlanCollection(config), new LogfileTable(config, source), new StubHourlyWindows());
    }

    private HBaseQueryImpl(
            final Config config,
            final ScanPlanCollection scanPlanCollection,
            final LogfileTable table,
            final HourlyWindows hourlyWindows
    ) {
        this.config = config;
        this.scanPlanCollection = scanPlanCollection;
        this.table = table;
        this.hourlyWindows = hourlyWindows;
    }

    @Override
    public void open(final long startOffset) {
        if (isOpen()) {
            return;
        }
        final List<View> views = scanPlanCollection.asViews(table);
        this.hourlyWindows = new HourlyWindowsImpl(views, startOffset);
    }

    @Override
    public void close() {
        if (isOpen()) {
            hourlyWindows.close();
            this.hourlyWindows = new StubHourlyWindows();
        }
    }

    @Override
    public boolean isOpen() {
        return !hourlyWindows.isStub();
    }

    @Override
    public boolean hasNext() {
        return !hourlyWindows.isStub() && hourlyWindows.hasNext();
    }

    @Override
    public List<Result> nextBatch() {
        if (!isOpen()) {
            throw new IllegalStateException("nextBatch() called before HBaseQuery was open");
        }
        final byte[] columnFamilyBytes = Bytes.toBytes("meta");
        final List<Result> nextBatchResults = hourlyWindows.nextHour();

        final long quantumLength = config.batchConfig.quantumLength;
        final long numPartitions = config.batchConfig.numPartitions;
        final long maxWeight = quantumLength * numPartitions;
        final long totalObjectCountLimit = config.batchConfig.totalObjectCountLimit;
        final float fileCompressionRatio = config.batchConfig.fileCompressionRatio;
        final float processingSpeed = config.batchConfig.processingSpeed;

        final List<Result> batchSizeLimitedResults = new ArrayList<>();
        if (!nextBatchResults.isEmpty()) {
            final BatchSizeLimit batchSizeLimit = new BatchSizeLimit(maxWeight, totalObjectCountLimit);
            for (final Result hourlyResult : nextBatchResults) {
                final long fileSize = Bytes.toLong(hourlyResult.getValue(columnFamilyBytes, Bytes.toBytes("fs")));
                final float hourlyResultEstimatedFileSize = fileSize * fileCompressionRatio / 1024 / 1024
                        / processingSpeed;
                batchSizeLimit.add(hourlyResultEstimatedFileSize);
                if (!batchSizeLimit.isOverLimit()) {
                    batchSizeLimitedResults.add(hourlyResult);
                }
                else {
                    LOGGER.info("Hourly results were over batch size limit, ignoring rest of the results");
                    break;
                }
            }
        }
        return batchSizeLimitedResults;
    }

    @Override
    public long earliest() {
        final List<ScanPlan> rangeList = scanPlanCollection.asList();
        final long earliest;
        if (rangeList.isEmpty()) {
            earliest = ZonedDateTime.now().minusHours(24).toEpochSecond();
        }
        else {
            long min = Long.MAX_VALUE;
            for (final ScanPlan range : rangeList) {
                min = Math.min(min, range.earliest());
            }
            earliest = min;
        }
        return earliest;
    }

    @Override
    public long latest() {
        return Math.max(latestCommited, earliest());
    }

    @Override
    public void commit(final long offset) {
        this.latestCommited = offset;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
