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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.teragrep.pth_06.ast.analyze.ScanPlan;
import com.teragrep.pth_06.ast.analyze.ScanPlanView;
import com.teragrep.pth_06.ast.analyze.ScanPlanCollection;
import com.teragrep.pth_06.ast.analyze.View;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.metrics.TaskMetric;
import com.teragrep.pth_06.planner.source.HBaseSource;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public final class HBaseQueryImpl implements HBaseQuery {

    private final ScanPlanCollection scanPlanCollection;
    private final LogfileTable table;
    private final MetricRegistry metricRegistry;
    private long latestCommited = Long.MIN_VALUE;
    private long mostRecent = Long.MIN_VALUE;

    public HBaseQueryImpl(final Config config, final HBaseSource source) {
        this(new ScanPlanCollection(config), new LogfileTable(config, source));
    }

    public HBaseQueryImpl(final ScanPlanCollection scanPlanCollection, final LogfileTable table) {
        this(scanPlanCollection, table, new MetricRegistry());
    }

    private HBaseQueryImpl(
            final ScanPlanCollection scanPlanCollection,
            final LogfileTable table,
            final MetricRegistry metricRegistry
    ) {
        this.scanPlanCollection = scanPlanCollection;
        this.table = table;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public long earliest() {
        final List<ScanPlan> rangeList = scanPlanCollection.asList();
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
    public CustomTaskMetric[] currentDatabaseMetrics() {
        // TODO update values
        final Snapshot latencySnapshot = metricRegistry.histogram("ArchiveDatabaseLatencyPerRow").getSnapshot();
        return new CustomTaskMetric[] {
                new TaskMetric("ArchiveDatabaseRowCount", metricRegistry.counter("ArchiveDatabaseRowCount").getCount()),
                new TaskMetric("ArchiveDatabaseRowMaxLatency", latencySnapshot.getMax()),
                new TaskMetric("ArchiveDatabaseRowAvgLatency", (long) latencySnapshot.getMean()),
                new TaskMetric("ArchiveDatabaseRowMinLatency", latencySnapshot.getMin()),
        };
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
        for (final ScanPlan range : scanPlanCollection.asList()) {
            views.add(new ScanPlanView(range, table));
        }
        return views;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
