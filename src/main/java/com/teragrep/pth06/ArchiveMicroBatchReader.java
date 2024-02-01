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

package com.teragrep.pth06;

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.pth06.scheduler.BatchSliceType;
import com.teragrep.pth06.scheduler.BatchScheduler;
import com.teragrep.pth06.scheduler.BatchSlice;
import com.teragrep.pth06.scheduler.NoOpScheduler;
import com.teragrep.pth06.scheduler.Scheduler;
import com.teragrep.pth06.task.ArchiveMicroBatchInputPartition;
import com.teragrep.pth06.task.KafkaMicroBatchInputPartition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

// logger

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h2>Archive Micro Batch Reader</h2>
 * Custom Data source reader class that reads batches of data from Spark data stream. Implements
 * Spark micro batch interface that allows for streaming reads.
 *
 * @see MicroBatchReader
 * @since 02/03/2021
 * @author Mikko Kortelainen
 */
class ArchiveMicroBatchReader implements MicroBatchReader {
    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveMicroBatchReader.class);

    // Active schema. Default is ArchiveSourceProvider.Schema
    private final StructType Schema;

    private final Scheduler scheduler;

    // db query results
    private LinkedList<LinkedList<BatchSlice>> currentBatch;

    private final Config config;

	/**
	 * Constructor for micro batch reader. Depending on the scheduler type set in config uses either NoOpScheduler
	 * or BatchScheduler.
	 * @see NoOpScheduler
	 * @see BatchScheduler
	 * @param schema schema that is used
	 * @param config config settings used to get schedulerType
	 * @throws IllegalStateException when scheduler type in config is not recognized
	 */
    ArchiveMicroBatchReader(StructType schema, Config config) {
        LOGGER.debug("ArchiveMicroBatchReader>");
        this.Schema = schema;
        // Execute  group mapper query
        this.config = config;
        // configurable  scheduler
        Config.SchedulerType schedulerType = config.getSchedulerType();
        
        switch (schedulerType) {
            case NOOP:
                scheduler = new NoOpScheduler(config);
                break;
            case BATCH:
                scheduler = new BatchScheduler(config);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + schedulerType);
        }

        LOGGER.info("MicroBatchReader> initialized with " + schedulerType);
    }

	/**
	 * * Used for testing.
	 *
	 * */
    @VisibleForTesting
    ArchiveMicroBatchReader(Scheduler scheduler, StructType schema, Config config) {
        // for offsetPlanner tests
        this.Schema = schema;
        this.config = config;
        this.scheduler = scheduler;
        LOGGER.info("@VisibleForTesting MicroBatchReader> initialized with scheduler: " + scheduler);
    }

	/** {@inheritDoc} */
    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {

        // start = lastFechId, stop = start + handler_num
        LOGGER.debug("ArchiveMicroBatchReader.setOffsetRange>" + start + "," + end);

        // initial run
        if (!start.isPresent() && !end.isPresent()) {
            currentBatch = scheduler.processInitial();
        }

        if (!start.isPresent() && end.isPresent()) {
            currentBatch = scheduler.processBefore(end.get());
        }

        if (start.isPresent() && !end.isPresent()) {
            currentBatch = scheduler.processAfter(start.get());
        }

        if (start.isPresent() && end.isPresent()) {
            currentBatch = scheduler.processRange(start.get(), end.get());
        }

        // Execute  Archive query
        LOGGER.info("----- Archive Query resultSetSize:" + currentBatch.size()
                + " Added values, start-end: " + scheduler.getStartOffset()
                + "-" + getEndOffset());
        LOGGER.debug("setOffsetRange(): rv: " + currentBatch);
    }

	/** {@inheritDoc} */
    @Override
    public Offset getStartOffset() {
        LOGGER.debug("ArchiveMicroBatchReader.getStartOffset> return " + scheduler.getStartOffset());
        return scheduler.getStartOffset();
    }

	/** {@inheritDoc} */
    @Override
    public Offset getEndOffset() {
        LOGGER.debug("ArchiveMicroBatchReader.getEndOffset> return " + scheduler.getEndOffset());
        return scheduler.getEndOffset();
    }

	/** {@inheritDoc} */
    @Override
    public Offset deserializeOffset(String s) {
        LOGGER.debug("ArchiveMicroBatchReader.deserializeOffset> " + s);
        return scheduler.deserializeOffset(s);
    }

	/** {@inheritDoc} */
    @Override
    public void commit(Offset offset) {
        LOGGER.debug("ArchiveMicroBatchReader.commit> " + offset);
        scheduler.commitOffsets(offset);
    }

	/** {@inheritDoc} */
    @Override
    public void stop() {
        LOGGER.debug("ArchiveMicroBatchReader.stop>");
    }

	/** {@inheritDoc} */
    @Override
    public StructType readSchema() {
        LOGGER.debug("ArchiveMicroBatchReader.readSchema>");
        return Schema;
    }

	/** {@inheritDoc} */
    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        LOGGER.debug("planInputPartitions(): currentBatch: " + currentBatch);

        List<InputPartition<InternalRow>> inputPartitions = new LinkedList<>();

        for (LinkedList<BatchSlice> taskObjectList : currentBatch) {
            LOGGER.debug("ArchiveMicroBatchReader.planInputPartitions> creating with startOffsetMap value: "
                    + scheduler.getStartOffset() + " endOffsetMap value: " + scheduler.getEndOffset()
                    + " planInputPartitions received record " + taskObjectList);

            // archive tasks
            LinkedList<ArchiveS3ObjectMetadata> archiveTaskList = new LinkedList<>();
            for (BatchSlice batchSlice : taskObjectList) {
                if (batchSlice.getBatchSliceType() == BatchSliceType.ARCHIVE) {
                    archiveTaskList.add(batchSlice.getArchiveS3ObjectMetadata());
                }
            }

            if (archiveTaskList.size() != 0) {
                inputPartitions.add(
                        new ArchiveMicroBatchInputPartition(
                                config.getS3endPoint(),
                                config.getS3identity(),
                                config.getS3credential(),
                                archiveTaskList,
                                config.getTeragrepAuditQuery(),
                                config.getTeragrepAuditReason(),
                                config.getTeragrepAuditUser(),
                                config.getTeragrepAuditPluginClassName(),
                                config.getSkipNonRFC5424Files()
                        )
                );
            }

            // kafka tasks
            for (BatchSlice batchSlice : taskObjectList) {
                if (batchSlice.getBatchSliceType() == BatchSliceType.KAFKA) {
                    inputPartitions.add(
                            new KafkaMicroBatchInputPartition(
                                    config.getKafkaExecutorOpts(),
                                    batchSlice.getKafkaTopicPartitionOffsetMetadata().getTopicPartition(),
                                    batchSlice.getKafkaTopicPartitionOffsetMetadata().getStartOffset(),
                                    batchSlice.getKafkaTopicPartitionOffsetMetadata().getEndOffset(),
                                    config.getKafkaExecutorConfig(),
                                    config.getSkipNonRFC5424Files() // reusing
                            )
                    );
                }
            }
        }
        LOGGER.debug("planInputPartitions(): rv: " + inputPartitions);
        return inputPartitions;
    }
}
