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

package com.teragrep.pth06.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.pth06.Config;
import com.teragrep.pth06.planner.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.jooq.Record10;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.LinkedList;
import java.util.Map;

/**
 * <h2>Batch Scheduler</h2>
 *
 * Class for scheduling execution of batches of data.
 *
 * @see Scheduler
 * @since 23/02/2022
 * @author Mikko Kortelainen
 */
public class BatchScheduler implements Scheduler {
    private final Logger LOGGER = LoggerFactory.getLogger(BatchScheduler.class);
    private final OffsetPlanner offsetPlanner;

    private final int numberOfpartitions;
    private final float quantumLength; // seconds


    public BatchScheduler(Config config) {
        this.offsetPlanner = new CombinedOffsetPlanner(config);

        this.numberOfpartitions = config.getNUM_PARTITIONS();
        this.quantumLength = config.getQuantumLength();
    }

    @VisibleForTesting
    public BatchScheduler(
            ArchiveQuery archiveQueryProcessor,
            KafkaQuery kafkaQueryProcessor
    ) {
        this.numberOfpartitions = 32;
        this.quantumLength = 15F;
        this.offsetPlanner = new CombinedOffsetPlanner(
                archiveQueryProcessor,
                kafkaQueryProcessor
        );
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processInitial() {
        LOGGER.debug("processInitial");
        BatchQueueManager batchQueueManager = new BatchQueueManager(numberOfpartitions, quantumLength);
        LinkedList<BatchSlice> slice = offsetPlanner.processInitial();

        if (slice.size() > 0) {
            batchQueueManager.addSlice(slice);

            while (batchQueueManager.hasTimeLeft()) {
                Offset endOffset = offsetPlanner.getEndOffset();
                slice = offsetPlanner.processAfter(endOffset);
                if (slice.size() > 0) {
                    batchQueueManager.addSlice(slice);
                } else {
                    break;
                }
            }
        }

        return batchQueueManager.getBatch();
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processBefore(Offset end) {
        LOGGER.debug("processBefore> " + end.json());

        BatchQueueManager batchQueueManager = new BatchQueueManager(numberOfpartitions, quantumLength);
        LinkedList<BatchSlice> slice = offsetPlanner.processBefore(end);
        if (slice.size() > 0) {
            batchQueueManager.addSlice(slice);
        }


        LOGGER.debug("BatchScheduler.processBefore> returning " + batchQueueManager.getBatch());

        return batchQueueManager.getBatch();
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processAfter(Offset start) {
        LOGGER.debug("processAfter> " + start.json());
        BatchQueueManager batchQueueManager = new BatchQueueManager(numberOfpartitions, quantumLength);
        LinkedList<BatchSlice> slice = offsetPlanner.processAfter(start);

        if (slice.size() > 0) {
            batchQueueManager.addSlice(slice);

            while (batchQueueManager.hasTimeLeft()) {
                Offset endOffset = offsetPlanner.getEndOffset();
                slice = offsetPlanner.processAfter(endOffset);
                if (slice.size() > 0) {
                    batchQueueManager.addSlice(slice);
                } else {
                    break;
                }
            }
        }

        return batchQueueManager.getBatch();
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processRange(Offset start, Offset end) {
        LOGGER.debug("processRange");
        BatchQueueManager batchQueueManager = new BatchQueueManager(numberOfpartitions, quantumLength);
        LinkedList<BatchSlice> slice = offsetPlanner.processRange(start, end);

        if (slice.size() > 0) {
            batchQueueManager.addSlice(slice);
        }

        return batchQueueManager.getBatch();
    }

    @Override
    public Offset getStartOffset() {
        return offsetPlanner.getStartOffset();
    }

    @Override
    public Offset getEndOffset() {
        return offsetPlanner.getEndOffset();
    }

    @Override
    public void commitOffsets(Offset offset) {
        LOGGER.debug("commitOffsets>" + offset.json());
        offsetPlanner.commitOffsets(offset);
    }

    @Override
    public Offset deserializeOffset(String s) {
        return offsetPlanner.deserializeOffset(s);
    }
}
