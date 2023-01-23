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

import java.sql.Date;
import java.util.LinkedList;
import java.util.Map;

/*
    NoOpScheduler only distributes the received dataset
    and leaves objects for Spark to schedule
 */

/**
 * <h1>No Op Scheduler</h1>
 *
 * Scheduler that only distributes the recived dataset
 * and leaves objects for Spark to schedule.
 *
 * @since 23/02/2022
 * @author Mikko Kortelainen
 */
public class NoOpScheduler implements Scheduler {

    private final OffsetPlanner offsetPlanner;

    public NoOpScheduler(Config config) {
        this.offsetPlanner = new CombinedOffsetPlanner(config);
    }

    @VisibleForTesting
    public NoOpScheduler(
            ArchiveQuery archiveQueryProcessor,
            KafkaQuery kafkaQueryProcessor
                         ) {
        this.offsetPlanner = new CombinedOffsetPlanner(
                archiveQueryProcessor,
                kafkaQueryProcessor
        );
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processInitial() {
        LinkedList<LinkedList<BatchSlice>> batch = new LinkedList<>();

        for (BatchSlice objectMetadata : offsetPlanner.processInitial()) {
            LinkedList<BatchSlice> taskObjectList = new LinkedList<>();
            taskObjectList.add(objectMetadata);
            batch.add(taskObjectList);
        }

        return batch;
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processBefore(Offset end) {
        LinkedList<LinkedList<BatchSlice>> batch = new LinkedList<>();

        for (BatchSlice objectMetadata : offsetPlanner.processBefore(end)) {
            LinkedList<BatchSlice> taskObjectList = new LinkedList<>();
            taskObjectList.add(objectMetadata);
            batch.add(taskObjectList);
        }

        return batch;
    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processAfter(Offset start) {
        LinkedList<LinkedList<BatchSlice>> batch = new LinkedList<>();

        for (BatchSlice objectMetadata : offsetPlanner.processAfter(start)) {
            LinkedList<BatchSlice> taskObjectList = new LinkedList<>();
            taskObjectList.add(objectMetadata);
            batch.add(taskObjectList);
        }

        return batch;

    }

    @Override
    public LinkedList<LinkedList<BatchSlice>> processRange(Offset start, Offset end) {
        LinkedList<LinkedList<BatchSlice>> batch = new LinkedList<>();

        for (BatchSlice objectMetadata : offsetPlanner.processRange(start, end)) {
            LinkedList<BatchSlice> taskObjectList = new LinkedList<>();
            taskObjectList.add(objectMetadata);
            batch.add(taskObjectList);
        }

        return batch;
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
        offsetPlanner.commitOffsets(offset);
    }

    @Override
    public Offset deserializeOffset(String s) {
        return offsetPlanner.deserializeOffset(s);
    }
}
