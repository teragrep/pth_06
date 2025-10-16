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
package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.planner.EpochFromRowKey;
import com.teragrep.pth_06.planner.LogfileTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ScanPlanView implements View {

    private final ScanPlan scanPlan;
    private final LogfileTable logfileTable;
    private long currentEpoch;
    private ResultScanner resultScanner;
    private Result bufferedResult;
    private boolean isOpen;
    private boolean isFinished;

    public ScanPlanView(final ScanPlan scanPlan, final LogfileTable logfileTable) {
        this.scanPlan = scanPlan;
        this.logfileTable = logfileTable;
        this.resultScanner = null;
        this.currentEpoch = scanPlan.earliest();
        this.isOpen = false;
        this.isFinished = false;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public void open() {
        if (scanPlan.isStub()) {
            throw new IllegalStateException("ScanRange was stub");
        }
        if (isOpen) {
            throw new IllegalStateException("called open() when ScanRangeView was already open");
        }
        else {
            try {
                resultScanner = logfileTable.table().getScanner(scanPlan.toScan());
                isOpen = true;
                isFinished = false;
                bufferedResult = null;
            }
            catch (final IOException e) {
                throw new RuntimeException("Error getting table: " + e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            resultScanner.close();
            isOpen = false;
        }
        isFinished = true;
    }

    @Override
    public boolean isEndOffsetWithinRange(final long offset) {
        return offset <= scanPlan.latest();
    }

    /**
     * Returns a new ScanRangeView from an updated offset start point.
     *
     * @param fromOffset start point for the updated view
     * @return ScanRangeView with same values as original but with updated start point
     * @throws IllegalArgumentException if offset is outside the scan range view
     */
    @Override
    public View viewFromOffset(final long fromOffset) {
        if (scanPlan.isStub()) {
            throw new IllegalStateException("ScanRange was stub");
        }
        if (fromOffset > scanPlan.latest()) {
            throw new IllegalArgumentException("fromOffset was later than the scan range latest");
        }
        return new ScanPlanView(scanPlan.rangeFromEarliest(fromOffset), logfileTable);
    }

    @Override
    public long latestEpochProcessed() {
        return currentEpoch;
    }

    /**
     * Returns results for the next window spanning the given duration in seconds from the last processed epoch.
     *
     * @param duration Duration in seconds to scan from the last processed epoch.
     * @return List of Results within the specified window duration.
     * @throws IOException              If an I/O error occurs during scanning.
     * @throws IllegalStateException    If the view is not open.
     * @throws IllegalArgumentException If duration is not positive.
     */
    @Override
    public List<Result> nextWindow(final long duration) throws IOException {
        if (scanPlan.isStub()) {
            throw new IllegalStateException("ScanRange was stub");
        }
        if (!isOpen) {
            throw new IllegalStateException("RangeView was closed");
        }
        if (duration < 1) {
            throw new IllegalArgumentException("given duration <" + duration + "> was not a positive");
        }

        final long windowEnd = currentEpoch + duration;
        final List<Result> results = new ArrayList<>();
        while (true) {
            final Result next;
            // check if buffer has a stored result
            if (bufferedResult != null) {
                final long bufferedResultRowKeyEpoch = new EpochFromRowKey(bufferedResult.getRow()).epoch();
                final boolean bufferedIsWithinWindow = bufferedResultRowKeyEpoch >= currentEpoch
                        && bufferedResultRowKeyEpoch < currentEpoch + duration;
                if (bufferedIsWithinWindow) {
                    next = bufferedResult;
                    bufferedResult = null;
                }
                else {
                    break;
                }
            }
            else {
                next = resultScanner.next();
            }
            if (next == null) {
                isFinished = true;
                break;
            }
            final long rowEpoch = new EpochFromRowKey(next.getRow()).epoch();

            if (rowEpoch >= windowEnd) {
                // store result that passed window end to buffer
                bufferedResult = next;
                break;
            }

            results.add(next);
        }
        currentEpoch += duration;
        return results;
    }
}
