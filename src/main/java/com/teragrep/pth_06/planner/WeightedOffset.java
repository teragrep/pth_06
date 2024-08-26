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
 * Contains the file size and offset of a given event. Used to estimate the weight of that event.
 */
final class WeightedOffset {

    private final long offset;
    private final long fileSize;

    final boolean isStub;

    /**
     * Creates a stub object. Used when the offset could not be found.
     */
    WeightedOffset() {
        this(0, 0, true);
    }

    /**
     * Creates a non-stub WeightedOffset object.
     * 
     * @param offset   Offset of event
     * @param fileSize File size of event
     */
    WeightedOffset(long offset, long fileSize) {
        this(offset, fileSize, false);
    }

    /**
     * Creates a WeightedOffset object. Not to be used outside of this class.
     * 
     * @param offset   Offset of the event
     * @param fileSize File size of the event
     * @param isStub   If this object is a stub object
     */
    private WeightedOffset(long offset, long fileSize, boolean isStub) {
        this.offset = offset;
        this.fileSize = fileSize;
        this.isStub = isStub;
    }

    /**
     * Estimates the weight of the event based on file size and processing speed.
     * 
     * @return Estimated weight
     * @param compressionRatio file compression ratio
     * @param processingSpeed  processing speed
     * @throws IllegalStateException if the object is a stub
     */
    float estimateWeight(final float compressionRatio, final float processingSpeed) {
        if (isStub) {
            throw new IllegalStateException("WeightedOffset is stub");
        }

        return (fileSize * compressionRatio) / 1024 / 1024 / processingSpeed;
    }

    /**
     * Returns the offset if the object is not a stub object.
     * 
     * @return offset
     * @throws IllegalStateException if the object is a stub
     */
    long offset() {
        if (isStub) {
            throw new IllegalStateException("WeightedOffset is stub");
        }
        return offset;
    }
}
