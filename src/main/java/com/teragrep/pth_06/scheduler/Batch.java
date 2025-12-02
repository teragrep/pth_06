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

import com.teragrep.pth_06.planner.ArchiveQuery;
import com.teragrep.pth_06.planner.HBaseQuery;
import com.teragrep.pth_06.planner.KafkaQuery;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * <h1>Batch</h1> Contains the necessary operations to form a Spark batch. It consists of Archive and/or Kafka data.
 * Each batch is constructed from a {@link BatchSliceCollection}, which in turn consists of multiple
 * {@link BatchSlice}s. Each of the slices contain the actual data.
 *
 * @author Eemeli Hukka
 */
public final class Batch extends LinkedList<LinkedList<BatchSlice>> {

    private final Logger LOGGER = LoggerFactory.getLogger(Batch.class);
    private long numberOfBatches = 0;
    private final LinkedList<BatchTaskQueue> runQueueArray;
    private final ArchiveQuery archiveQuery;
    private final KafkaQuery kafkaQuery;
    private final HBaseQuery hbaseQuery;

    public Batch(long numOfPartitions, ArchiveQuery archiveQuery, KafkaQuery kafkaQuery, HBaseQuery hbaseQuery) {
        this.runQueueArray = new LinkedList<>();

        for (int i = 0; i < numOfPartitions; i++) {
            this.runQueueArray.add(new BatchTaskQueue());
        }

        this.archiveQuery = archiveQuery;
        this.kafkaQuery = kafkaQuery;
        this.hbaseQuery = hbaseQuery;
    }

    public Batch processRange(Offset start, Offset end) {
        LOGGER.debug("processRange start <{}>, end <{}>", start, end);

        final BatchSliceCollection slice;
        if (useHBase() && useKafka()) {
            slice = new HBaseBatchSliceCollection(hbaseQuery).processRange(start, end);
            slice.addAll(new KafkaBatchSliceCollection(kafkaQuery).processRange(start, end));
        }
        else if (useArchive() && useKafka()) {
            slice = new ArchiveBatchSliceCollection(archiveQuery).processRange(start, end);
            slice.addAll(new KafkaBatchSliceCollection(kafkaQuery).processRange(start, end));
        }
        else if (useHBase()) {
            slice = new HBaseBatchSliceCollection(hbaseQuery).processRange(start, end);
        }
        else if (useArchive()) {
            slice = new ArchiveBatchSliceCollection(archiveQuery).processRange(start, end);
        }
        else if (useKafka()) {
            slice = new KafkaBatchSliceCollection(kafkaQuery).processRange(start, end);
        }
        else {
            throw new IllegalStateException("No datasource enabled for batch");
        }

        if (!slice.isEmpty()) {
            this.addSlice(slice);
        }

        return this.getBatch();
    }

    public void addSlice(LinkedList<BatchSlice> sliceCollection) {
        numberOfBatches++;

        while (!sliceCollection.isEmpty()) {
            BatchSlice longestObject = null;

            // find the longest object
            for (BatchSlice objectMetadata : sliceCollection) {
                if (longestObject == null) {
                    longestObject = objectMetadata;
                }
                else {
                    if (longestObject.getSize() > objectMetadata.getSize()) {
                        longestObject = objectMetadata;
                    }
                }
            }

            // object found
            if (longestObject != null) {
                sliceCollection.remove(longestObject);

                // find shortest queue
                BatchTaskQueue shortestQueue = null;
                for (BatchTaskQueue btq : runQueueArray) {
                    if (shortestQueue == null) {
                        shortestQueue = btq;
                    }
                    else {
                        if (shortestQueue.getQueueTime() > btq.getQueueTime()) {
                            shortestQueue = btq;
                        }
                    }
                }
                if (shortestQueue != null) {
                    shortestQueue.add(longestObject);
                }
                else {
                    throw new RuntimeException("shortestQueue was null");
                }

            }
        }
    }

    public Batch getBatch() {
        this.clear(); // clear on getBatch?
        for (BatchTaskQueue btq : runQueueArray) {
            this.add(btq.getQueue());
        }
        LOGGER.debug("getBatch: " + this);
        return this;
    }

    private boolean useHBase() {
        return !hbaseQuery.isStub();
    }

    private boolean useArchive() {
        return !archiveQuery.isStub();
    }

    private boolean useKafka() {
        return !kafkaQuery.isStub();
    }
}
