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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.teragrep.pth_06.ast.analyze.ScanPlan;
import com.teragrep.pth_06.ast.analyze.ScanPlanCollection;
import com.teragrep.pth_06.ast.analyze.View;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.metrics.TaskMetric;
import com.teragrep.pth_06.planner.source.HBaseSource;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Responsible for splitting results from scanPlanCollection into batches
 */
public final class HBaseQueryImpl implements HBaseQuery, QueryMetrics {

    private final Logger LOGGER = LoggerFactory.getLogger(HBaseQueryImpl.class);

    private final Config config;
    private final ScanPlanCollection scanPlanCollection;
    private final LogfileTable table;
    private final MetricRegistry metricRegistry;
    private LimitedResults limitedResults;
    private long mostRecentCommitedOffset = Long.MIN_VALUE;
    private HourlySlices hourlySlices;

    public HBaseQueryImpl(final Config config, final HBaseSource source) {
        this(
                config,
                new ScanPlanCollection(config),
                new LogfileTable(config, source),
                new MetricRegistry(),
                new StubHourlySlices(),
                new StubLimitedResults()
        );
    }

    private HBaseQueryImpl(
            final Config config,
            final ScanPlanCollection scanPlanCollection,
            final LogfileTable table,
            final MetricRegistry metricRegistry,
            final HourlySlices hourlySlices,
            final LimitedResults limitedResults
    ) {
        this.config = config;
        this.scanPlanCollection = scanPlanCollection;
        this.table = table;
        this.metricRegistry = metricRegistry;
        this.hourlySlices = hourlySlices;
        this.limitedResults = limitedResults;
    }

    /**
     * Creates open scans for the query, starting from the provided offset
     * 
     * @param startOffset - earliest offset included in the results
     */
    @Override
    public void open(final long startOffset) {
        if (isOpen()) {
            LOGGER.info("Closing open HBaseQuery and re-opening at offset <{}>", startOffset);
            close();
        }
        final List<View> views = scanPlanCollection.asViews(table);
        this.hourlySlices = new HourlyViewsSlices(views, startOffset);
    }

    /**
     * Closes all open scans in the hourly slices
     */
    @Override
    public void close() {
        if (isOpen()) {
            hourlySlices.close();
            this.hourlySlices = new StubHourlySlices();
            this.limitedResults = new StubLimitedResults();
        }
    }

    @Override
    public boolean isOpen() {
        return !hourlySlices.isStub();
    }

    /**
     * Returns the current batch, this value is updated using the latest() method
     * 
     * @return List<Result> - results for the current batch
     */
    @Override
    public List<Result> currentBatch() {
        if (limitedResults.isStub()) {
            throw new IllegalStateException("current batch was not yet processed, latest() was not called");
        }
        return limitedResults.results();
    }

    /**
     * Finds the earliest offset available in the scan plan collection, formed from the query
     * 
     * @return long - earliest offset queried
     */
    @Override
    public long earliest() {
        final List<ScanPlan> rangeList = scanPlanCollection.asList();
        if (rangeList.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot determine earliest value, no scan plans were available for query - index might not exist"
            );
        }
        long earliest = Long.MAX_VALUE;
        for (final ScanPlan range : rangeList) {
            earliest = Math.min(earliest, range.earliest());
        }
        LOGGER.debug("earliest() called with offset <{}>", earliest);
        return earliest;
    }

    /**
     * Queries results form hbase until the batch limit is reached and updates latest
     * 
     * @return long - latest offset after the next batch is filled
     */
    @Override
    public long latest() {
        long startOffset;
        if (mostRecentCommitedOffset == Long.MIN_VALUE) {
            startOffset = earliest();
        }
        else {
            startOffset = mostRecentCommitedOffset;
        }
        if (!isOpen()) {
            LOGGER.info("latest() called advancing query results from start offset <{}>", startOffset);
            open(startOffset);
        }
        final long quantumLength = config.batchConfig.quantumLength;
        final long numPartitions = config.batchConfig.numPartitions;
        final long maxWeight = quantumLength * numPartitions;
        final long totalObjectCountLimit = config.batchConfig.totalObjectCountLimit;
        final Timer.Context timerCtx = metricRegistry.timer("ArchiveDatabaseLatency").time();
        final BatchSizeLimit batchSizeLimit = new BatchSizeLimit(maxWeight, totalObjectCountLimit);
        final long previousBatchOffset;
        if (!limitedResults.isStub()) {
            previousBatchOffset = limitedResults.latest();
        }
        else {
            previousBatchOffset = mostRecentOffset();
        }
        // update the current batch results
        this.limitedResults = new BatchSizeLimitedResults(
                hourlySlices,
                batchSizeLimit,
                config,
                previousBatchOffset,
                new MetricRegistry()
        );
        // update metrics
        long rows = limitedResults.results().size();
        final long latencyNs = timerCtx.stop();
        if (rows != 0) {
            metricRegistry.histogram("ArchiveDatabaseLatencyPerRow").update(latencyNs / rows);
        }
        metricRegistry.counter("ArchiveDatabaseRowCount").inc(rows);
        final long latest = limitedResults.latest();
        return latest;
    }

    @Override
    public void commit(final long offset) {
        mostRecentCommitedOffset = offset;
        LOGGER.debug("Commited to offset <{}>", offset);
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public long mostRecentOffset() {
        long offset;
        if (limitedResults.isStub()) {
            offset = earliest();
        }
        else {
            offset = limitedResults.latest();
        }
        return offset;
    }

    @Override
    public CustomTaskMetric[] currentDatabaseMetrics() {
        final Snapshot latencySnapshot = metricRegistry.histogram("ArchiveDatabaseLatencyPerRow").getSnapshot();
        return new CustomTaskMetric[] {
                new TaskMetric("ArchiveDatabaseRowCount", metricRegistry.counter("ArchiveDatabaseRowCount").getCount()),
                new TaskMetric("ArchiveDatabaseRowMaxLatency", latencySnapshot.getMax()),
                new TaskMetric("ArchiveDatabaseRowAvgLatency", (long) latencySnapshot.getMean()),
                new TaskMetric("ArchiveDatabaseRowMinLatency", latencySnapshot.getMin()),
        };
    }
}
