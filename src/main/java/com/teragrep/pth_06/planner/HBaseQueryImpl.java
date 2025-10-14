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

import com.teragrep.pth_06.ast.analyze.ScanPlan;
import com.teragrep.pth_06.ast.analyze.ScanRangeView;
import com.teragrep.pth_06.ast.analyze.ScanRangeCollection;
import com.teragrep.pth_06.ast.analyze.View;
import com.teragrep.pth_06.config.Config;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public final class HBaseQueryImpl implements HBaseQuery {

    private final ScanRangeCollection scanRangeCollection;
    private final LogfileTable table;
    private long latestCommited = Long.MIN_VALUE;
    private long mostRecent = Long.MIN_VALUE;

    public HBaseQueryImpl(final Config config) {
        this(new ScanRangeCollection(config), new LogfileTable(config));
    }

    public HBaseQueryImpl(final ScanRangeCollection scanRangeCollection, final LogfileTable table) {
        this.scanRangeCollection = scanRangeCollection;
        this.table = table;
    }

    @Override
    public long earliest() {
        final List<ScanPlan> rangeList = scanRangeCollection.asList();
        final long earliest;
        if (rangeList.isEmpty()) {
            earliest = ZonedDateTime.now().minusHours(24).toEpochSecond();
        }
        else {
            long min = Long.MAX_VALUE;
            for (final ScanPlan range : rangeList) {
                min = Math.min(min, range.earliest());
            }
            earliest = min;
        }
        return earliest;
    }

    @Override
    public long latest() {
        final long latest;
        if (latestCommited == Long.MIN_VALUE) {
            latest = earliest();
        }
        else {
            latest = latestCommited + 3600L;
        }
        return latest;
    }

    @Override
    public long mostRecentOffset() {
        return mostRecent;
    }

    @Override
    public void updateMostRecent(final long offset) {
        this.mostRecent = offset;
    }

    @Override
    public void commit(final long offset) {
        this.latestCommited = offset;
    }

    @Override
    public List<View> openViews() {
        final List<View> views = new ArrayList<>();
        for (final ScanPlan range : scanRangeCollection.asList()) {
            views.add(new ScanRangeView(range, table));
        }
        return views;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
