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
    private final Config config;
    private final ArchiveQuery archiveQuery;
    private final KafkaQuery kafkaQuery;

    public Batch(Config config, ArchiveQuery aq, KafkaQuery kq) {
        this.config = config;
        this.runQueueArray = new LinkedList<>();

        for (int i = 0; i < config.batchConfig.numPartitions; i++) {
            this.runQueueArray.add(new BatchTaskQueue());
        }

        this.archiveQuery = aq;
        this.kafkaQuery = kq;
    }

    public Batch processRange(Offset start, Offset end) {
        LOGGER.debug("processRange");

        BatchSliceCollection slice = null;
        if (config.isArchiveEnabled) {
            slice = new ArchiveBatchSliceCollection(this.archiveQuery).processRange(start, end);
        }

        if (config.isKafkaEnabled) {
            if (slice == null) {
                slice = new KafkaBatchSliceCollection(this.kafkaQuery).processRange(start, end);
            }
            else {
                slice.addAll(new KafkaBatchSliceCollection(this.kafkaQuery).processRange(start, end));
            }
        }
        if (slice != null && !slice.isEmpty()) {
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

}
