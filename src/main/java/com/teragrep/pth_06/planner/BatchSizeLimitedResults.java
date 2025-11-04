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
import com.codahale.metrics.Timer;
import com.teragrep.pth_06.config.BatchConfig;
import com.teragrep.pth_06.config.Config;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public final class BatchSizeLimitedResults implements LimitedResults {

    private final HourlySlices slices;
    private final BatchSizeLimit limit;
    private final List<Result> results;
    private final float fileCompressionRatio;
    private final float processingSpeed;
    private final long startingOffset;
    private final MetricRegistry metricRegistry;

    public BatchSizeLimitedResults(
            final HourlySlices slices,
            final BatchSizeLimit limit,
            final Config config,
            final long startingOffset,
            MetricRegistry metricRegistry
    ) {
        this(slices, limit, config.batchConfig, startingOffset, metricRegistry);
    }

    public BatchSizeLimitedResults(
            final HourlySlices slices,
            final BatchSizeLimit limit,
            final BatchConfig batchConfig,
            final long startingOffset,
            MetricRegistry metricRegistry
    ) {
        this(
                slices,
                limit,
                batchConfig.fileCompressionRatio,
                batchConfig.processingSpeed,
                startingOffset,
                metricRegistry
        );
    }

    public BatchSizeLimitedResults(
            final HourlySlices slices,
            final BatchSizeLimit limit,
            final float fileCompressionRatio,
            final float processingSpeed,
            final long startingOffset,
            MetricRegistry metricRegistry
    ) {
        this(slices, limit, new ArrayList<>(), fileCompressionRatio, processingSpeed, startingOffset, metricRegistry);
    }

    public BatchSizeLimitedResults(
            final HourlySlices slices,
            final BatchSizeLimit limit,
            final List<Result> results,
            final float fileCompressionRatio,
            final float processingSpeed,
            final long startingOffset,
            MetricRegistry metricRegistry
    ) {
        this.slices = slices;
        this.limit = limit;
        this.results = results;
        this.fileCompressionRatio = fileCompressionRatio;
        this.processingSpeed = processingSpeed;
        this.startingOffset = startingOffset;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public List<Result> results() {
        if (results.isEmpty()) {
            long latencyNs;
            final byte[] columnFamilyBytes = Bytes.toBytes("meta");
            try (final Timer.Context timerCtx = metricRegistry.timer("ArchiveDatabaseLatency").time()) {
                while (!limit.isOverLimit() && !slices.isStub() && slices.hasNext()) {
                    final List<Result> nextHourResults = slices.nextHour();
                    for (final Result hourlyResult : nextHourResults) {
                        final long fileSize = Bytes
                                .toLong(hourlyResult.getValue(columnFamilyBytes, Bytes.toBytes("fs")));
                        final float hourlyResultEstimatedFileSize = fileSize * fileCompressionRatio / 1024 / 1024
                                / processingSpeed;
                        limit.add(hourlyResultEstimatedFileSize);
                        results.add(hourlyResult);
                        if (limit.isOverLimit()) {
                            break;
                        }
                    }
                }
                latencyNs = timerCtx.stop();
            }
            // update metrics
            final long rows = results.size();
            if (rows > 0) {
                metricRegistry.histogram("ArchiveDatabaseLatencyPerRow").update(latencyNs / rows);
            }
            metricRegistry.counter("ArchiveDatabaseRowCount").inc(rows);
        }
        return results;
    }

    @Override
    public long latest() {
        results();
        long latest;
        if (!results.isEmpty()) {
            Result lastResult = results.get(results().size() - 1);
            latest = new EpochFromRowKey(lastResult.getRow()).epoch();
        }
        else {
            latest = startingOffset;
        }
        return latest;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
