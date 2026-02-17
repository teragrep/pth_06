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
import com.teragrep.pth_06.planner.*;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import com.teragrep.pth_06.planner.offset.KafkaOffset;
import com.teragrep.pth_06.scheduler.*;
import com.teragrep.pth_06.task.ArchiveMicroBatchInputPartition;
import com.teragrep.pth_06.task.TeragrepPartitionReaderFactory;
import com.teragrep.pth_06.task.KafkaMicroBatchInputPartition;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;

import java.io.IOException;
import java.util.ArrayList;
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
    private final ArchiveQuery aq;
    private final KafkaQuery kq;
    private final BatchCalculator batchCalculator;

    /**
     * Constructor for ArchiveMicroStreamReader
     *
     * @param config Datasource configuration object
     */
    ArchiveMicroStreamReader(Config config) {
        this.LOGGER = new ConfiguredLogger(classLogger, config.loggingConfig.isDebug());
        LOGGER.debug("ArchiveMicroStreamReader ctor called");

        this.config = config;

        if (config.isArchiveEnabled) {
            this.aq = new ArchiveQueryProcessor(config);
        }
        else {
            this.aq = null;
        }

        if (config.isKafkaEnabled) {
            this.kq = new KafkaQueryProcessor(config);
        }
        else {
            this.kq = null;
        }

        this.batchCalculator = new BatchCalculator(this.config, this.aq, this.kq);
        LOGGER.debug("ArchiveMicroStreamReader ctor exit");
    }

    /**
     * Used for testing.
     */
    @VisibleForTesting
    ArchiveMicroStreamReader(ArchiveQuery aq, KafkaQuery kq, Config config) {
        this.LOGGER = new ConfiguredLogger(classLogger, config.loggingConfig.isDebug());
        LOGGER.debug("ArchiveMicroStreamReader test ctor called");

        this.config = config;
        this.aq = aq;
        this.kq = kq;

        this.batchCalculator = new BatchCalculator(this.config, this.aq, this.kq);
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
        final DatasourceOffset rv;
        if (this.config.isArchiveEnabled && !this.config.isKafkaEnabled) {
            // only archive
            rv = new DatasourceOffset(new LongOffset(this.aq.getInitialOffset() - 3600L));
        }
        else if (!this.config.isArchiveEnabled && this.config.isKafkaEnabled) {
            // only kafka
            rv = new DatasourceOffset(new KafkaOffset(this.kq.getBeginningOffsets(null)));
        }
        else if (this.config.isArchiveEnabled) {
            // both
            rv = new DatasourceOffset(
                    new LongOffset(this.aq.getInitialOffset() - 3600L),
                    new KafkaOffset(this.kq.getBeginningOffsets(null))
            );
        }
        else {
            // neither
            throw new IllegalStateException("no datasources enabled, can't get initial offset");
        }
        LOGGER.debug("ArchiveMicroStreamReader.initialOffset returns <{}>", rv);
        return rv;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Offset deserializeOffset(String json) {
        LOGGER.debug("ArchiveMicroStreamReader.deserializeOffset json <{}>", json);
        final DatasourceOffset offset = new DatasourceOffset(json);
        LOGGER.debug("ArchiveMicroStreamReader.deserializeOffset deserialized <{}>", offset);
        return offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(Offset offset) {
        LOGGER.debug("ArchiveMicroStreamReader.commit offset <{}>", offset);
        if (this.config.isArchiveEnabled) {
            this.aq.commit(((DatasourceOffset) offset).getArchiveOffset().offset());
        }
        LOGGER.debug("ArchiveMicroStreamReader.commit exit");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOGGER.debug("ArchiveMicroStreamReader.stop called");
        if (this.config.isKafkaEnabled) {
            try {
                kq.close();
            }
            catch (final IOException exception) {
                LOGGER
                        .error(
                                "Error closing kafka query while calling stop() with message <{}>",
                                exception.getMessage()
                        );
            }
        }
    }

    /**
     * Used when Spark progresses the query further to fetch more data.
     *
     * @return {@link DatasourceOffset} object containing all necessary offsets for the enabled datasources.
     */
    @Override
    public Offset latestOffset() {
        LOGGER.debug("ArchiveMicroStreamReader.latestOffset called");
        DatasourceOffset rv;
        if (this.config.isArchiveEnabled && !this.config.isKafkaEnabled) {
            // only archive
            rv = new DatasourceOffset(new LongOffset(this.aq.incrementAndGetLatestOffset()));
        }
        else if (!this.config.isArchiveEnabled && this.config.isKafkaEnabled) {
            // only kafka
            rv = new DatasourceOffset(new KafkaOffset(this.kq.getInitialEndOffsets()));
        }
        else if (this.config.isArchiveEnabled) {
            // both
            rv = new DatasourceOffset(
                    new LongOffset(this.aq.incrementAndGetLatestOffset()),
                    new KafkaOffset(this.kq.getInitialEndOffsets())
            );
        }
        else {
            // neither
            throw new IllegalStateException("no datasources enabled, can't get latest offset");
        }

        LOGGER.debug("ArchiveMicroStreamReader.latestOffset returns <{}>", rv);
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
        LOGGER.debug("ArchiveMicroStreamReader.planInputPartitions: start <{}>, end <{}>", start, end);
        List<InputPartition> inputPartitions = new ArrayList<>();

        List<List<BatchUnit>> currentBatch = batchCalculator.processRange(start, end);

        for (List<BatchUnit> taskObjectList : currentBatch) {

            // archive tasks
            LinkedList<ArchiveS3ObjectMetadata> archiveTaskList = new LinkedList<>();
            for (BatchUnit batchUnit : taskObjectList) {
                if (batchUnit.type.equals(BatchUnit.Type.ARCHIVE)) {
                    archiveTaskList.add(batchUnit.archiveS3ObjectMetadata);
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
                                        config.archiveConfig.skipNonRFC5424Files,
                                        config.archiveConfig.epochMigrationMode
                                )
                        );
            }

            // kafka tasks
            for (BatchUnit batchUnit : taskObjectList) {
                if (batchUnit.type.equals(BatchUnit.Type.KAFKA)) {
                    inputPartitions
                            .add(
                                    new KafkaMicroBatchInputPartition(
                                            config.kafkaConfig.executorOpts,
                                            batchUnit.kafkaTopicPartitionOffsetMetadata.topicPartition,
                                            batchUnit.kafkaTopicPartitionOffsetMetadata.startOffset,
                                            batchUnit.kafkaTopicPartitionOffsetMetadata.endOffset,
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
        final DatasourceOffset rv;
        if (config.isArchiveEnabled && config.isKafkaEnabled) {
            rv = new DatasourceOffset(
                    new LongOffset(this.aq.mostRecentOffset()),
                    new KafkaOffset(this.kq.getInitialEndOffsets())
            );
        }
        else if (config.isArchiveEnabled) {
            rv = new DatasourceOffset(new LongOffset(this.aq.mostRecentOffset()));
        }
        else if (config.isKafkaEnabled) {
            rv = new DatasourceOffset(new KafkaOffset(this.kq.getInitialEndOffsets()));
        }
        else {
            throw new IllegalStateException("No datasources enabled, can't get last used offset");
        }
        LOGGER.debug("ArchiveMicroStreamReader.mostRecentOffset returns <{}>", rv);
        return rv;
    }

    public CustomTaskMetric[] currentDatabaseMetrics() {
        LOGGER.debug("ArchiveMicroStreamReader.currentDatabaseMetrics called");
        final CustomTaskMetric[] metrics;
        if (aq != null) {
            metrics = aq.currentDatabaseMetrics();
        }
        else {
            metrics = new CustomTaskMetric[0];
        }
        LOGGER.debug("ArchiveMicroStreamReader.currentDatabaseMetrics returns <{}> metrics", metrics.length);
        return metrics;
    }
}
