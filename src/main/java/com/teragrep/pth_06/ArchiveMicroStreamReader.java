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
package com.teragrep.pth_06;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.*;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import com.teragrep.pth_06.planner.offset.HdfsOffset;
import com.teragrep.pth_06.planner.offset.KafkaOffset;
import com.teragrep.pth_06.scheduler.*;
import com.teragrep.pth_06.task.ArchiveMicroBatchInputPartition;
import com.teragrep.pth_06.task.TeragrepPartitionReaderFactory;
import com.teragrep.pth_06.task.KafkaMicroBatchInputPartition;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;

import java.util.LinkedList;
import java.util.List;

// logger

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>Archive Micro Stream Reader</h1> Custom Spark Structured Streaming Datasource that reads data from Archive and
 * Kafka in micro-batches.
 *
 * @see MicroBatchStream
 * @since 02/03/2021
 * @author Mikko Kortelainen
 * @author Eemeli Hukka
 */
public final class ArchiveMicroStreamReader implements MicroBatchStream {

    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveMicroStreamReader.class);

    /**
     * Contains the configurations given as options when loading from this datasource.
     */
    private final Config config;
    private final ArchiveQuery aq;
    private final KafkaQuery kq;
    private final HdfsQuery hq;
    private final JsonArray hdfsOffsets;

    /**
     * Constructor for ArchiveMicroStreamReader
     * 
     * @param config Datasource configuration object
     */
    ArchiveMicroStreamReader(Config config) {
        LOGGER.debug("ArchiveMicroBatchReader>");

        this.config = config;

        if (config.isHdfsEnabled) {
            this.hq = new HdfsQueryProcessor(config);
            hdfsOffsets = hq.hdfsOffsetMapToJSON();
        }
        else {
            this.hq = null;
            hdfsOffsets = null;
        }

        if (config.isArchiveEnabled) {
            this.aq = new ArchiveQueryProcessor(config);
        }
        else {
            this.aq = null;
        }

        if (config.isKafkaEnabled) {
            if (config.isHdfsEnabled) {
                this.kq = new KafkaQueryProcessor(config);
                this.kq.seekToHdfsOffsets(hdfsOffsets);
            }
            else {
                this.kq = new KafkaQueryProcessor(config);
            }
        }
        else {
            this.kq = null;
        }

        LOGGER.debug("MicroBatchReader> initialized");
    }

    /**
     * Used for testing.
     */
    @VisibleForTesting
    ArchiveMicroStreamReader(HdfsQuery hq, ArchiveQuery aq, KafkaQuery kq, Config config) {
        this.config = config;
        this.aq = aq; // Uses its own hardcoded query string defined in MockTeragrepDatasource.
        this.kq = kq; // Skips using query string (and thus topic filtering) altogether.
        this.hq = hq; // Uses the query string from config for topic filtering.
        if (this.hq != null && this.kq != null) {
            hdfsOffsets = this.hq.hdfsOffsetMapToJSON();
            this.kq.seekToHdfsOffsets(hdfsOffsets);
        }
        else {
            hdfsOffsets = null;
        }

        LOGGER.debug("@VisibleForTesting MicroBatchReader> initialized");
    }

    /**
     * Used when Spark requests the initial offset when starting a new query.
     * 
     * @return {@link DatasourceOffset} object containing all necessary offsets for the enabled datasources.
     * @throws IllegalStateException if no datasources were enabled
     */
    @Override
    public Offset initialOffset() {
        // After rebase is complete: Refactor the DatasourceOffset and SerializedDatasourceOffset if the 8x else-if statements are too much.

        // archive only: subtract 3600s (1 hour) from earliest to return first row (start exclusive)
        DatasourceOffset rv;
        if (this.config.isArchiveEnabled && !this.config.isKafkaEnabled && !this.config.isHdfsEnabled) {
            // only archive
            rv = new DatasourceOffset(new LongOffset(this.aq.getInitialOffset() - 3600L));
        }
        else if (!this.config.isArchiveEnabled && this.config.isKafkaEnabled && !this.config.isHdfsEnabled) {
            // only kafka
            rv = new DatasourceOffset(new KafkaOffset(this.kq.getBeginningOffsets(null)));
        }
        else if (!this.config.isArchiveEnabled && !this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // only HDFS
            rv = new DatasourceOffset(new HdfsOffset(this.hq.getBeginningOffsets().getOffsetMap()));
        }
        else if (this.config.isArchiveEnabled && this.config.isKafkaEnabled && !this.config.isHdfsEnabled) {
            // kafka and archive
            rv = new DatasourceOffset(
                    new LongOffset(this.aq.getInitialOffset() - 3600L),
                    new KafkaOffset(this.kq.getBeginningOffsets(null))
            );
        }
        else if (this.config.isArchiveEnabled && !this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // archive and HDFS
            rv = new DatasourceOffset(
                    new HdfsOffset(this.hq.getBeginningOffsets().getOffsetMap()),
                    new LongOffset(this.aq.getInitialOffset() - 3600L)
            );
        }
        else if (!this.config.isArchiveEnabled && this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // Kafka and HDFS, check if any files are available from HDFS.
            if (hdfsOffsets.size() > 0) {
                rv = new DatasourceOffset(
                        new HdfsOffset(this.hq.getBeginningOffsets().getOffsetMap()),
                        new KafkaOffset(this.kq.getConsumerPositions(hdfsOffsets))
                );
            }
            else {
                rv = new DatasourceOffset(
                        new HdfsOffset(this.hq.getBeginningOffsets().getOffsetMap()),
                        new KafkaOffset(this.kq.getBeginningOffsets(null))
                );
            }
        }
        else if (this.config.isArchiveEnabled && this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // all three, check if any files are available from HDFS.
            if (hdfsOffsets.size() > 0) {
                rv = new DatasourceOffset(
                        new HdfsOffset(this.hq.getBeginningOffsets().getOffsetMap()),
                        new LongOffset(this.aq.getInitialOffset() - 3600L),
                        new KafkaOffset(this.kq.getConsumerPositions(hdfsOffsets))
                );
            }
            else {
                rv = new DatasourceOffset(
                        new HdfsOffset(this.hq.getBeginningOffsets().getOffsetMap()),
                        new LongOffset(this.aq.getInitialOffset() - 3600L),
                        new KafkaOffset(this.kq.getBeginningOffsets(null))
                );
            }
        }
        else {
            // none
            throw new IllegalStateException("no datasources enabled, can't get initial offset");
        }
        LOGGER.debug("offset[initial]= {}", rv);
        return rv;
    }

    /** {@inheritDoc} */
    @Override
    public Offset deserializeOffset(String json) {
        return new DatasourceOffset(json);
    }

    /** {@inheritDoc} */
    @Override
    public void commit(Offset offset) {
        if (this.config.isArchiveEnabled) {
            this.aq.commit(((DatasourceOffset) offset).getArchiveOffset().offset());
        }
        if (this.config.isHdfsEnabled) {
            this.hq.commit(((DatasourceOffset) offset).getHdfsOffset());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        LOGGER.debug("ArchiveMicroStreamReader.stop>");
    }

    /**
     * Used when Spark progresses the query further to fetch more data.
     * 
     * @return {@link DatasourceOffset} object containing all necessary offsets for the enabled datasources.
     */
    @Override
    public Offset latestOffset() {
        // After rebase is complete: Refactor the DatasourceOffset and SerializedDatasourceOffset if the 8x else-if statements are too much.

        DatasourceOffset rv;
        if (this.config.isArchiveEnabled && !this.config.isKafkaEnabled && !this.config.isHdfsEnabled) {
            // only archive
            rv = new DatasourceOffset(new LongOffset(this.aq.incrementAndGetLatestOffset()));
        }
        else if (!this.config.isArchiveEnabled && this.config.isKafkaEnabled && !this.config.isHdfsEnabled) {
            // only kafka
            rv = new DatasourceOffset(new KafkaOffset(this.kq.getInitialEndOffsets()));
        }
        else if (!this.config.isArchiveEnabled && !this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // only hdfs
            rv = new DatasourceOffset(new HdfsOffset(this.hq.getInitialEndOffsets().getOffsetMap()));
        }
        else if (this.config.isArchiveEnabled && this.config.isKafkaEnabled && !this.config.isHdfsEnabled) {
            // kafka and archive
            rv = new DatasourceOffset(
                    new LongOffset(this.aq.incrementAndGetLatestOffset()),
                    new KafkaOffset(this.kq.getInitialEndOffsets())
            );
        }
        else if (this.config.isArchiveEnabled && !this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // archive and hdfs
            rv = new DatasourceOffset(
                    new HdfsOffset(this.hq.getInitialEndOffsets().getOffsetMap()),
                    new LongOffset(this.aq.incrementAndGetLatestOffset())
            );
        }
        else if (!this.config.isArchiveEnabled && this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // Kafka and HDFS
            rv = new DatasourceOffset(
                    new HdfsOffset(this.hq.getInitialEndOffsets().getOffsetMap()),
                    new KafkaOffset(this.kq.getInitialEndOffsets())
            );
        }
        else if (this.config.isArchiveEnabled && this.config.isKafkaEnabled && this.config.isHdfsEnabled) {
            // all three
            rv = new DatasourceOffset(
                    new HdfsOffset(this.hq.getInitialEndOffsets().getOffsetMap()),
                    new LongOffset(this.aq.incrementAndGetLatestOffset()),
                    new KafkaOffset(this.kq.getInitialEndOffsets())
            );
        }
        else {
            // none
            throw new IllegalStateException("no datasources enabled, can't get latest offset");
        }

        LOGGER.debug("offset[latest]= {}", rv);
        return rv;
    }

    /**
     * Forms the batch between start and end offsets and forms the input partitions from that batch.
     * 
     * @param start Start offset
     * @param end   End offset
     * @return InputPartitions as an array
     */
    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        List<InputPartition> inputPartitions = new LinkedList<>();

        Batch currentBatch = new Batch(config, hq, aq, kq).processRange(start, end);

        for (LinkedList<BatchSlice> taskObjectList : currentBatch) {

            // archive tasks
            LinkedList<ArchiveS3ObjectMetadata> archiveTaskList = new LinkedList<>();
            for (BatchSlice batchSlice : taskObjectList) {
                if (batchSlice.type.equals(BatchSlice.Type.ARCHIVE)) {
                    archiveTaskList.add(batchSlice.archiveS3ObjectMetadata);
                }
            }

            if (!archiveTaskList.isEmpty()) {
                inputPartitions
                        .add(
                                new ArchiveMicroBatchInputPartition(
                                        config.archiveConfig.s3EndPoint,
                                        config.archiveConfig.s3Identity,
                                        config.archiveConfig.s3Credential,
                                        archiveTaskList,
                                        config.auditConfig.query,
                                        config.auditConfig.reason,
                                        config.auditConfig.user,
                                        config.auditConfig.pluginClassName,
                                        config.archiveConfig.skipNonRFC5424Files
                                )
                        );
            }

            // HDFS tasks
            LinkedList<HdfsTopicPartitionOffsetMetadata> hdfsTaskList = new LinkedList<>();
            for (BatchSlice batchSlice : taskObjectList) {
                if (batchSlice.type.equals(BatchSlice.Type.HDFS)) {
                    hdfsTaskList.add(batchSlice.hdfsTopicPartitionOffsetMetadata);
                }
            }

            if (!hdfsTaskList.isEmpty()) {
                // BatchSliceType.HDFS contains the metadata for the HDFS files that contain the records that are being queried. Available topics in HDFS are already filtered based on the spark query conditions.
                // The records that are inside the files are fetched and processed in the tasker. Tasker does rest of the filtering based on the given query conditions, for example the cutoff epoch handling between the records that are fetched from S3 and HDFS.
                // The Spark planner/scheduler is only single-threaded while tasker is multithreaded. Planner is not suitable for fetching and processing all the records, it should be done in tasker which will handle the processing in multithreaded environment based on batch slices.

                // Implement HdfsMicroBatchInputPartition usage here. If needed change the slice creation to be incremental like it is with archive, for that create HDFS variant of the incrementAndGetLatestOffset()-method from ArchiveQueryProcessor.
                // At the moment it fetches all the metadata available at once and puts them into hdfsTaskList.
                /*inputPartitions.add(new HdfsMicroBatchInputPartition(
                        config.hdfsConfig,
                        hdfsTaskList
                ));*/
            }

            // kafka tasks
            for (BatchSlice batchSlice : taskObjectList) {
                if (batchSlice.type.equals(BatchSlice.Type.KAFKA)) {
                    inputPartitions
                            .add(
                                    new KafkaMicroBatchInputPartition(
                                            config.kafkaConfig.executorOpts,
                                            batchSlice.kafkaTopicPartitionOffsetMetadata.topicPartition,
                                            batchSlice.kafkaTopicPartitionOffsetMetadata.startOffset,
                                            batchSlice.kafkaTopicPartitionOffsetMetadata.endOffset,
                                            config.kafkaConfig.executorConfig,
                                            config.kafkaConfig.skipNonRFC5424Records
                                    )
                            );
                }
            }
        }

        return inputPartitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new TeragrepPartitionReaderFactory(config.isMetadataQuery);
    }
}
