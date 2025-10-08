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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class ScanRangeImpl implements ScanRange {

    private final long streamId;
    private final long earliest;
    private final long latest;
    private final FilterList filterList;

    public ScanRangeImpl(final long streamId, final long earliest, final long latest, final FilterList filterList) {
        this.streamId = streamId;
        this.earliest = earliest;
        this.latest = latest;
        this.filterList = filterList;
    }

    @Override
    public Scan toScan() {
        final ByteBuffer startBuffer = ByteBuffer.allocate(16); // first 2 long values
        startBuffer.putLong(streamId);
        startBuffer.putLong(earliest);
        final ByteBuffer stopBuffer = ByteBuffer.allocate(16); // first 2 long values
        stopBuffer.putLong(streamId);
        stopBuffer.putLong(latest);
        final Scan scan = new Scan().withStartRow(startBuffer.array()).withStopRow(stopBuffer.array());
        scan.setFilter(filterList);
        return scan;
    }

    @Override
    public ScanRange rangeFromEarliest(long earliestLimit) {
        if (earliest < earliestLimit && earliestLimit < latest) {
            return new ScanRangeImpl(streamId, earliestLimit, latest, filterList);
        }
        else {
            return this;
        }
    }

    @Override
    public ScanRange rangeUntilLatest(long latestLimit) {
        if (earliest < latestLimit && latestLimit < latest) {
            return new ScanRangeImpl(streamId, earliest, latestLimit, filterList);
        }
        else {
            return this;
        }
    }

    @Override
    public ScanRange toRangeBetween(long earliestLimit, long latestLimit) {
        boolean rangeIntersects = new ScanRangeImpl(streamId, earliestLimit - 1, latestLimit + 1, filterList)
                .intersects(this);
        ScanRange result;
        if (rangeIntersects) {
            long updatedEarliest = earliest;
            long updatedLatest = latest;
            if (earliestLimit > earliest) {
                updatedEarliest = earliestLimit;
            }
            if (latestLimit < latest) {
                updatedLatest = latestLimit;
            }
            if (updatedEarliest == updatedLatest) {
                result = new StubScanRange();
            }
             else if (updatedEarliest > updatedLatest) {
                result = new StubScanRange();
            } else {
                result = new ScanRangeImpl(streamId, updatedEarliest, updatedLatest, filterList);
            }
        }
        else {
            result = new StubScanRange();
        }
        return result;
    }

    public boolean intersects(final ScanRange other) {
        if (!Objects.equals(this.streamId, other.streamId()) || !filterList.equals(other.filterList())) {
            return false;
        }
        return this.earliest <= other.latest() && other.earliest() <= this.latest;
    }

    public ScanRangeImpl merge(final ScanRange other) {
        if (intersects(other)) {
            final Long minEarliest = Math.min(earliest, other.earliest());
            final Long maxLatest = Math.max(latest, other.latest());
            return new ScanRangeImpl(streamId, minEarliest, maxLatest, filterList);
        }
        else {
            throw new IllegalArgumentException("Unable to merge ranges did not intersect");
        }
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final ScanRangeImpl scanRangeImpl = (ScanRangeImpl) object;
        return Objects.equals(streamId, scanRangeImpl.streamId) && Objects.equals(earliest, scanRangeImpl.earliest)
                && Objects.equals(latest, scanRangeImpl.latest) && Objects.equals(filterList, scanRangeImpl.filterList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, earliest, latest, filterList);
    }

    @Override
    public String toString() {
        return String.format("ScanRange id: <%s> between <%s> - <%s>", streamId, earliest, latest);
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public long streamId() {
        return streamId;
    }

    @Override
    public long earliest() {
        return earliest;
    }

    @Override
    public long latest() {
        return latest;
    }

    @Override
    public FilterList filterList() {
        return filterList;
    }
}
