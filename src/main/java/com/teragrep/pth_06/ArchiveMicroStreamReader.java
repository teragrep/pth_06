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
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.ArchiveQuery;
import com.teragrep.pth_06.planner.HBaseQuery;
import com.teragrep.pth_06.planner.KafkaQuery;
import com.teragrep.pth_06.planner.factory.ArchiveQueryFactory;
import com.teragrep.pth_06.planner.factory.Factory;
import com.teragrep.pth_06.planner.factory.HBaseQueryFactory;
import com.teragrep.pth_06.planner.factory.KafkaQueryFactory;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import com.teragrep.pth_06.planner.offset.KafkaOffset;
import com.teragrep.pth_06.scheduler.Batch;
import com.teragrep.pth_06.scheduler.BatchSlice;
import com.teragrep.pth_06.task.ArchiveMicroBatchInputPartition;
import com.teragrep.pth_06.task.TeragrepPartitionReaderFactory;
import com.teragrep.pth_06.task.KafkaMicroBatchInputPartition;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
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
 * @author Mikko Kortelainen
 * @author Eemeli Hukka
 * @see MicroBatchStream
 * @since 02/03/2021
 */
public final class ArchiveMicroStreamReader implements MicroBatchStream {

    private static final Logger classLogger = LoggerFactory.getLogger(ArchiveMicroStreamReader.class);
    private final ConfiguredLogger LOGGER;
    /**
     * Contains the configurations given as options when loading from this datasource.
     */
    private final Config config;
    private final ArchiveQuery archiveQuery;
    private final KafkaQuery kafkaQuery;
    private final HBaseQuery hBaseQuery;

    /**
     * Constructor for ArchiveMicroStreamReader
     *
     * @param config Datasource configuration object
     */
    ArchiveMicroStreamReader(final Config config) {
        this(config, new ArchiveQueryFactory(config), new KafkaQueryFactory(config), new HBaseQueryFactory(config));
    }

    ArchiveMicroStreamReader(
            final Config config,
            final Factory<ArchiveQuery> archiveQueryFactory,
            final Factory<KafkaQuery> kafkaQueryFactory,
            final Factory<HBaseQuery> hbaseQueryFactory
    ) {
        this(config, archiveQueryFactory.object(), kafkaQueryFactory.object(), hbaseQueryFactory.object());
    }

    /**
     * Used for testing.
     */
    @VisibleForTesting
    ArchiveMicroStreamReader(
            final Config config,
            final ArchiveQuery archiveQuery,
            final KafkaQuery kafkaQuery,
            final HBaseQuery hbaseQuery
    ) {
        this.LOGGER = new ConfiguredLogger(classLogger, config.loggingConfig.isDebug());
        LOGGER.debug("ArchiveMicroStreamReader test ctor called");
        this.config = config;
        this.archiveQuery = archiveQuery;
        this.kafkaQuery = kafkaQuery;
        this.hBaseQuery = hbaseQuery;

        LOGGER.debug("ArchiveMicroStreamReader test ctor exit");
    }

    /**
     * Used when Spark requests the initial offset when starting a new query.
     *
     * @return {@link DatasourceOffset} object containing all necessary offsets for the enabled datasources.
     * @throws IllegalStateException if no datasources were enabled
     */
    @Override
    public Offset initialOffset() {
        LOGGER.debug("ArchiveMicroStreamReader.initialOffset called");
        // archive only: subtract 3600s (1 hour) from earliest to return first row (start exclusive)
        final DatasourceOffset datasourceOffset;
        if (useHBase() && useKafka()) {
            datasourceOffset = new DatasourceOffset(
                    new LongOffset(hBaseQuery.earliest()),
                    new KafkaOffset(kafkaQuery.getBeginningOffsets(null))
            );
        }
        else if (useArchive() && useKafka()) {
            datasourceOffset = new DatasourceOffset(
                    new LongOffset(archiveQuery.getInitialOffset() - 3600L),
                    new KafkaOffset(kafkaQuery.getBeginningOffsets(null))
            );
        }
        else if (useHBase()) {
            datasourceOffset = new DatasourceOffset(new LongOffset(hBaseQuery.earliest()));
        }
        else if (useArchive()) {
            datasourceOffset = new DatasourceOffset(new LongOffset(archiveQuery.getInitialOffset() - 3600L));
        }
        else if (useKafka()) {
            datasourceOffset = new DatasourceOffset(new KafkaOffset(kafkaQuery.getBeginningOffsets(null)));
        }
        else {
            throw new IllegalStateException("no datasources enabled, can't get initial offset");
        }
        LOGGER.debug("ArchiveMicroStreamReader.initialOffset returns <{}>", datasourceOffset);
        return datasourceOffset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Offset deserializeOffset(final String json) {
        LOGGER.debug("ArchiveMicroStreamReader.deserializeOffset json <{}>", json);
        final DatasourceOffset offset  = new DatasourceOffset(json);
        LOGGER.debug("ArchiveMicroStreamReader.deserializeOffset deserialized <{}>", offset);
        return offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(final Offset offset) {
        LOGGER.debug("ArchiveMicroStreamReader.commit offset <{}>", offset);
        final long offsetLongValue = ((DatasourceOffset) offset).getArchiveOffset().offset();
        if (useHBase()) {
            hBaseQuery.commit(offsetLongValue);
        }
        else if (useArchive()) {
            archiveQuery.commit(offsetLongValue);
        }
        else {
            LOGGER.debug("Archive datasource was not enabled, commit() call ignored");
        }
        LOGGER.debug("ArchiveMicroStreamReader.commit exit");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOGGER.debug("ArchiveMicroStreamReader.stop called");
    }

    /**
     * Used when Spark progresses the query further to fetch more data.
     *
     * @return {@link DatasourceOffset} object containing all necessary offsets for the enabled datasources.
     */
    @Override
    public Offset latestOffset() {
        LOGGER.debug("ArchiveMicroStreamReader.latestOffset called");
        final DatasourceOffset datasourceOffset;
        if (useHBase() && useKafka()) {
            datasourceOffset = new DatasourceOffset(
                    new LongOffset(hBaseQuery.latest()),
                    new KafkaOffset(kafkaQuery.getInitialEndOffsets())
            );
        }
        else if (useArchive() && useKafka()) {
            datasourceOffset = new DatasourceOffset(
                    new LongOffset(archiveQuery.incrementAndGetLatestOffset()),
                    new KafkaOffset(kafkaQuery.getInitialEndOffsets())
            );
        }
        else if (useHBase()) {
            datasourceOffset = new DatasourceOffset(new LongOffset(hBaseQuery.latest()));
        }
        else if (useArchive()) {
            datasourceOffset = new DatasourceOffset(new LongOffset(archiveQuery.incrementAndGetLatestOffset()));
        }
        else if (useKafka()) {
            datasourceOffset = new DatasourceOffset(new KafkaOffset(kafkaQuery.getInitialEndOffsets()));
        }
        else {
            throw new IllegalStateException("no datasources enabled, can't get latest offset");
        }
        LOGGER.debug("ArchiveMicroStreamReader.latestOffset returns <{}>", datasourceOffset);
        return datasourceOffset;
    }

    /**
     * Forms the batch between start and end offsets and forms the input partitions from that batch.
     *
     * @param start Start offset
     * @param end   End offset
     * @return InputPartitions as an array
     */
    @Override
    public InputPartition[] planInputPartitions(final Offset start, final Offset end) {
        LOGGER.debug("ArchiveMicroStreamReader.planInputPartitions: start <{}>, end <{}>", start, end);
        final List<InputPartition> inputPartitions = new LinkedList<>();

        final int numPartitions = config.batchConfig.numPartitions;
        final Batch currentBatch = new Batch(numPartitions, archiveQuery, kafkaQuery, hBaseQuery)
                .processRange(start, end);

        for (final LinkedList<BatchSlice> taskObjectList : currentBatch) {

            // archive tasks
            final LinkedList<ArchiveS3ObjectMetadata> archiveTaskList = new LinkedList<>();
            for (final BatchSlice batchSlice : taskObjectList) {
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

            // kafka tasks
            for (final BatchSlice batchSlice : taskObjectList) {
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

        LOGGER
                .debug(
                        "ArchiveMicroStreamReader.planInputPartitions: returned <{}> inputPartitions",
                        inputPartitions.size()
                );
        return inputPartitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        LOGGER.debug("ArchiveMicroStreamReader.createReaderFactory called");
        final TeragrepPartitionReaderFactory readerFactory = new TeragrepPartitionReaderFactory(config.isMetadataQuery);
        LOGGER.debug("ArchiveMicroStreamReader.createReaderFactory returned <{}>", readerFactory);
        return readerFactory;
    }

    public DatasourceOffset mostRecentOffset() {
        LOGGER.debug("ArchiveMicroStreamReader.mostRecentOffset called");
        final DatasourceOffset datasourceOffset;
        if (useHBase() && useKafka()) {
            datasourceOffset = new DatasourceOffset(
                    new LongOffset(hBaseQuery.mostRecentOffset()),
                    new KafkaOffset(kafkaQuery.getInitialEndOffsets())
            );
        }
        else if (useArchive() && useKafka()) {
            datasourceOffset = new DatasourceOffset(
                    new LongOffset(archiveQuery.mostRecentOffset()),
                    new KafkaOffset(kafkaQuery.getInitialEndOffsets())
            );
        }
        else if (useHBase()) {
            datasourceOffset = new DatasourceOffset(new LongOffset(hBaseQuery.mostRecentOffset()));
        }
        else if (useArchive()) {
            datasourceOffset = new DatasourceOffset(new LongOffset(archiveQuery.mostRecentOffset()));
        }
        else if (useKafka()) {
            datasourceOffset = new DatasourceOffset(new KafkaOffset(kafkaQuery.getInitialEndOffsets()));
        }
        else {
            throw new IllegalStateException("No datasources enabled, can't get last used offset");
        }
        LOGGER.debug("ArchiveMicroStreamReader.mostRecentOffset returns <{}>", datasourceOffset);
        return datasourceOffset;
    }

    public CustomTaskMetric[] currentDatabaseMetrics() {
        LOGGER.debug("ArchiveMicroStreamReader.currentDatabaseMetrics called");
        final CustomTaskMetric[] metrics;
        if (useHBase()) {
            metrics = hBaseQuery.currentDatabaseMetrics();
        }
        else if (useArchive()) {
            metrics = archiveQuery.currentDatabaseMetrics();
        }
        else {
            metrics = new CustomTaskMetric[0];
        }
        LOGGER.debug("ArchiveMicroStreamReader.currentDatabaseMetrics returns <{}> metrics", metrics.length);
        return metrics;
    }

    private boolean useHBase() {
        return !hBaseQuery.isStub();
    }

    private boolean useArchive() {
        return !archiveQuery.isStub();
    }

    private boolean useKafka() {
        return !kafkaQuery.isStub();
    }
}
