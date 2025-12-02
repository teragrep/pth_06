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

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.ArchiveQuery;
import com.teragrep.pth_06.planner.KafkaQuery;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * <h1>Batch</h1> Contains the necessary operations to form a Spark batch. It consists of Archive and/or Kafka data.
 * Each batch is constructed from a {@link RangeProcessor}, which in turn consists of multiple {@link BatchUnit}s. Each
 * of the slices contain the actual data.
 * 
 * @author Eemeli Hukka, Mikko Kortelainen
 */
public final class BatchCalculator {

    private final Logger LOGGER = LoggerFactory.getLogger(BatchCalculator.class);
    private final Config config;
    private final ArchiveQuery archiveQuery;
    private final KafkaQuery kafkaQuery;

    public BatchCalculator(Config config, ArchiveQuery aq, KafkaQuery kq) {
        this.config = config;

        this.archiveQuery = aq;
        this.kafkaQuery = kq;
    }

    public LinkedList<LinkedList<BatchUnit>> processRange(Offset start, Offset end) {
        LOGGER.debug("processRange");

        LinkedList<BatchUnit> slice = new LinkedList<>();

        if (config.isArchiveEnabled) {
            slice.addAll(new ArchiveRangeProcessor(this.archiveQuery).processRange(start, end));
        }

        if (config.isKafkaEnabled) {
            slice.addAll(new KafkaRangeProcessor(this.kafkaQuery).processRange(start, end));
        }

        return buildBatch(slice);

    }

    private LinkedList<LinkedList<BatchUnit>> buildBatch(LinkedList<BatchUnit> sliceCollection) {

        final PriorityQueue<BatchTaskQueue> runQueues = new PriorityQueue<>();

        for (int i = 0; i < config.batchConfig.numPartitions; i++) {
            runQueues.add(new BatchTaskQueue());
        }

        PriorityQueue<BatchUnit> batchUnitQueue = new PriorityQueue<>(sliceCollection);

        while (!batchUnitQueue.isEmpty()) {
            BatchUnit longestObject = batchUnitQueue.poll();

            if (!runQueues.isEmpty()) {
                BatchTaskQueue shortestQueque = runQueues.poll();
                shortestQueque.add(longestObject);
                runQueues.add(shortestQueque);
            }
        }

        final LinkedList<LinkedList<BatchUnit>> taskSliceQueues = new LinkedList<>();

        for (BatchTaskQueue btq : runQueues) {
            taskSliceQueues.add(btq.getQueue());
        }

        LOGGER.debug("getBatch: " + taskSliceQueues);
        return taskSliceQueues;
    }
}
