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
package com.teragrep.pth_06.scheduler;
/**
 * @class HBaseBatchSliceCollection
 * @brief extends BatchSliceCollection
 *
 * @responsibilities
 * - creates a collection of batch slices from a hbase query results
 * - ensures a batch limit is observed in batches
 * - ensures that hourly results are synchronized across multiple indexes
 *
 * @collaborators
 * - HBaseQuery
 * - SynchronizedHourlyResults
 * - BatchSizeLimit
 * - Config
 *
 * @startuml
 * class HBaseBatchSliceCollection {
 *     + processRange(Offset start, Offset end)
 * }
 *
 * HBaseBatchSliceCollection --> HBaseQuery : results source
 * HBaseBatchSliceCollection --> SynchronizedHourlyResults : syncs results for hour
 * HBaseBatchSliceCollection --> BatchSizeLimit : limits the results per hour
 * HBaseBatchSliceCollection --> Config : options for used objects
 *
 * Collaborators:
 * - HBaseQuery
 * - SynchronizedHourlyResults
 * - BatchSizeLimit
 * - Config
 *
 * @enduml
 */

import com.teragrep.pth_06.ArchiveS3ObjectMetadata;
import com.teragrep.pth_06.ast.analyze.ScanRangeView;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.BatchSizeLimit;
import com.teragrep.pth_06.planner.HBaseQuery;
import com.teragrep.pth_06.planner.SynchronizedHourlyResults;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates batches results from a HBase query for a desired offset range
 */
public final class HBaseBatchSliceCollection extends BatchSliceCollection {

    private final Logger LOGGER = LoggerFactory.getLogger(HBaseBatchSliceCollection.class);
    private final HBaseQuery hBaseQuery;
    private final byte[] meta; // column family
    private final int quantumLength;
    private final int numPartitions;
    private final long totalObjectCountLimit;
    private final float fileCompressionRatio;
    private final float processingSpeed;

    public HBaseBatchSliceCollection(final HBaseQuery hBaseQuery, final Config config) {
        super();
        this.hBaseQuery = hBaseQuery;
        this.meta = Bytes.toBytes("meta");
        this.quantumLength = config.batchConfig.quantumLength;
        this.numPartitions = config.batchConfig.numPartitions;
        this.totalObjectCountLimit = config.batchConfig.totalObjectCountLimit;
        this.fileCompressionRatio = config.batchConfig.fileCompressionRatio;
        this.processingSpeed = config.batchConfig.processingSpeed;
    }

    /**
     * Creates batches from query results for the provided offset range
     */
    public HBaseBatchSliceCollection processRange(final Offset start, final Offset end) {
        this.clear(); // clear internal list
        final long startOffsetLong = ((DatasourceOffset) start).getArchiveOffset().offset();
        final long endOffsetLong = ((DatasourceOffset) end).getArchiveOffset().offset();
        LOGGER.debug("processRange() start <{}> end <{}>", startOffsetLong, endOffsetLong);
        final List<ScanRangeView> scanRangeViews = hBaseQuery.openViews();
        final SynchronizedHourlyResults synchronizedHourlyResults = new SynchronizedHourlyResults(
                scanRangeViews,
                startOffsetLong
        );
        final List<Result> results = new ArrayList<>();
        final long maxWeight = (long) quantumLength * numPartitions;
        while (synchronizedHourlyResults.hasNext()) {
            final List<Result> hourlyResults = synchronizedHourlyResults.nextHour();
            final long currentEpoch = synchronizedHourlyResults.currentEpoch();
            if (!hourlyResults.isEmpty()) {
                final BatchSizeLimit batchSizeLimit = new BatchSizeLimit(maxWeight, totalObjectCountLimit);
                for (final Result hourlyResult : hourlyResults) {
                    final long fileSize = Bytes.toLong(hourlyResult.getValue(meta, Bytes.toBytes("fs")));
                    final float hourlyResultEstimatedFileSize = fileSize * fileCompressionRatio / 1024 / 1024
                            / processingSpeed;
                    batchSizeLimit.add(hourlyResultEstimatedFileSize);
                    if (!batchSizeLimit.isOverLimit()) {
                        results.add(hourlyResult);
                    }
                    else {
                        LOGGER
                                .info(
                                        "Hourly results were over batch size limit with <{}> logfiles, ignoring rest of the results",
                                        hourlyResult.size()
                                );
                        break;
                    }
                }
            }
            hBaseQuery.updateLatest(currentEpoch);
        }

        for (final Result result : results) {
            final String id = Bytes.toString(result.getValue(meta, Bytes.toBytes("i")));
            final String directory = Bytes.toString(result.getValue(meta, Bytes.toBytes("d")));
            final String stream = Bytes.toString(result.getValue(meta, Bytes.toBytes("s")));
            final String host = Bytes.toString(result.getValue(meta, Bytes.toBytes("h")));
            final String bucket = Bytes.toString(result.getValue(meta, Bytes.toBytes("b")));
            final String path = Bytes.toString(result.getValue(meta, Bytes.toBytes("p")));
            final long logtime = Bytes.toLong(result.getValue(meta, Bytes.toBytes("t")));
            final long filesize = ULong.valueOf(Bytes.toLong(result.getValue(meta, Bytes.toBytes("fs")))).longValue();
            final byte[] uncompressedFileSizeBytes = result.getValue(meta, Bytes.toBytes("ufs"));
            final long uncompressedFileSize;

            // uncompressed can be null, null values are replicated as empty byte arrays to hbase
            if (uncompressedFileSizeBytes.length == 0) {
                uncompressedFileSize = -1L;
            }
            else {
                uncompressedFileSize = ULong.valueOf(Bytes.toLong(uncompressedFileSizeBytes)).longValue();
            }

            this
                    .add(
                            new BatchSlice(
                                    new ArchiveS3ObjectMetadata(
                                            id,
                                            bucket,
                                            path,
                                            directory,
                                            stream,
                                            host,
                                            logtime,
                                            filesize,
                                            uncompressedFileSize
                                    )
                            )
                    );
        }

        return this;
    }
}
