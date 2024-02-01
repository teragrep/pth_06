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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * <h2>Batch Queue Manager</h2>
 *
 * Class for handling a queue of batch tasks.
 *
 * @see BatchTaskQueue
 * @since 23/02/2022
 * @author Mikko Kortelainen
 */
public class BatchQueueManager {
    private final Logger LOGGER = LoggerFactory.getLogger(BatchQueueManager.class);

    private final int numberOfpartitions;
    private final float quantumLength; // seconds

    private final LinkedList<BatchTaskQueue> runQueueArray;

    private long numberOfBatches = 0;

    BatchQueueManager(int numberOfpartitions, float quantumLength) {
        this.numberOfpartitions = numberOfpartitions;
        this.quantumLength = quantumLength;

        runQueueArray = new LinkedList<>();

        for (int i = 1 ; i <= numberOfpartitions; i++ ) {
            runQueueArray.add(new BatchTaskQueue());
        }
    }

    void addSlice(LinkedList<BatchSlice> slice) {
        LOGGER.debug("adding " + slice);
        numberOfBatches = numberOfBatches + 1;
        while (slice.size() > 0) {
            BatchSlice longestObject = null;

            // find longest object
            for (BatchSlice objectMetadata : slice) {
                if (longestObject == null) {
                    longestObject = objectMetadata;
                } else {
                    if (longestObject.getSize() > objectMetadata.getSize()) {
                        longestObject = objectMetadata;
                    }
                }
            }

            // an object is found
            if (longestObject != null) {
                slice.remove(longestObject);

                // find shortest queue
                BatchTaskQueue shortestQueue = null;
                for (BatchTaskQueue btq : runQueueArray) {
                    if (shortestQueue == null) {
                        shortestQueue = btq;
                    } else {
                        if (shortestQueue.getQueueTime() > btq.getQueueTime()) {
                            shortestQueue = btq;
                        }
                    }
                }
                assert shortestQueue != null;
                shortestQueue.add(longestObject);
            }
        }
    }

    // return if at least one slice would fit
    boolean hasTimeLeft() {
        boolean rv;

        // calculate if there is time left within the run queues
        float totalTime = 0F;
        for (BatchTaskQueue btq : runQueueArray) {
            totalTime = totalTime + btq.getQueueTime();
        }

        float runTime = totalTime / numberOfpartitions;
        float estimatedTime = runTime + (runTime/numberOfBatches);
        LOGGER.debug("hasTimeLeft time based estimate (quantumLength - estimatedTime): " + (quantumLength - estimatedTime));
        rv = estimatedTime < quantumLength;

        /*
         alternative is that objects are large and go over the quantumLength
         and there are still empty BatchTaskQueues
         */

        long emptyQueues = 0L;
        for (BatchTaskQueue btq : runQueueArray) {
            if (btq.getQueueTime() == 0F) {
                emptyQueues = emptyQueues + 1;
            }
        }

        if (emptyQueues >= numberOfBatches) {
            // there is still space for one batch
            rv = true;
        }

        /*
        alternative is that too many batches exist
         */
        if (numberOfBatches >= 100) {
            rv = false;
        }

        LOGGER.debug("hasTimeLeft returning: " + rv);
        return rv;
    }

    LinkedList<LinkedList<BatchSlice>> getBatch() {
        LinkedList<LinkedList<BatchSlice>> batch = new LinkedList<>();
        for (BatchTaskQueue btq : runQueueArray) {
            batch.add(btq.getQueue());
        }
        LOGGER.debug("getBatch: " + batch);
        return batch;
    }
}
