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
package com.teragrep.pth_06.planner;

/**
 * Class for checking the batch size and ensuring that it does not grow over the given weight and total object count
 * limit.
 */
public final class BatchSizeLimit {

    /** Maximum weight of the batch (length of executor queue * amount of executors) */
    final long maxWeight;

    /** Maximum count of total objects, in case the weights of the objects are small */
    final long maxObjectCount;

    /** accumulated sum of weights */
    private float accumulatedWeight;

    /** accumulated sum of object count */
    private long accumulatedObjectCount;

    /**
     * Initialize the BatchSizeLimit with the given maximum size and maximum object count.
     * 
     * @param maxWeight      quantumLength * numPartitions
     * @param maxObjectCount maximum objects per batch
     */
    public BatchSizeLimit(final long maxWeight, final long maxObjectCount) {
        this.maxWeight = maxWeight;
        this.maxObjectCount = maxObjectCount;
        this.accumulatedWeight = 0F;
        this.accumulatedObjectCount = 0L;
    }

    /**
     * Accumulate the given weight and count of objects
     * 
     * @param weight weight of offset delta
     */
    public void add(float weight) {
        accumulatedWeight += weight;
        accumulatedObjectCount++;
    }

    /**
     * check if accumulated weight is over the maximum size limit OR if the accumulated count of objects is over the
     * limit
     * 
     * @return if weight or object count is over the given limit
     */
    public boolean isOverLimit() {
        final boolean tooHeavyWeight = accumulatedWeight > maxWeight;
        final boolean tooManyObjects = accumulatedObjectCount > maxObjectCount;

        return tooManyObjects || tooHeavyWeight;
    }

    @Override
    public String toString() {
        return "BatchSizeLimit[weight="
                .concat(String.valueOf(this.accumulatedWeight))
                .concat("/")
                .concat(String.valueOf(this.maxWeight))
                .concat(", objects=")
                .concat(String.valueOf(this.accumulatedObjectCount))
                .concat("/")
                .concat(String.valueOf(this.maxObjectCount))
                .concat("]");
    }
}
