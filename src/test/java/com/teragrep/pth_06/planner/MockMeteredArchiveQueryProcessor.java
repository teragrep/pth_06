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
import com.codahale.metrics.Timer;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.SettableGauge;
import com.teragrep.pth_06.metrics.TaskMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;

import java.sql.Date;

public final class MockMeteredArchiveQueryProcessor implements ArchiveQuery {

    private final MetricRegistry metricRegistry;
    private final ArchiveQuery archiveQuery;

    public MockMeteredArchiveQueryProcessor(final String query) {
        this(new MockArchiveQueryProcessor(query));
    }

    public MockMeteredArchiveQueryProcessor(final ArchiveQuery archiveQuery) {
        this.archiveQuery = archiveQuery;
        this.metricRegistry = new MetricRegistry();
    }

    @Override
    public Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> processBetweenUnixEpochHours(
            long startHour,
            long endHour
    ) {

        final Timer.Context timerCtx = metricRegistry.timer("mockRowsTime").time();
        final Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> rv;
        final long latencyNs;

        try {
            rv = archiveQuery.processBetweenUnixEpochHours(startHour, endHour);
        }
        finally {
            latencyNs = timerCtx.stop();
            timerCtx.close();
        }

        if (!rv.isEmpty()) {
            metricRegistry.histogram("mockRowTime").update(latencyNs / rv.size());
        }
        SettableGauge<Long> count = metricRegistry.gauge("mockRowCount");
        count.setValue((long) rv.size());
        return rv;
    }

    @Override
    public void commit(long offset) {
        archiveQuery.commit(offset);
    }

    @Override
    public Long incrementAndGetLatestOffset() {
        return archiveQuery.incrementAndGetLatestOffset();
    }

    @Override
    public Long mostRecentOffset() {
        return archiveQuery.mostRecentOffset();
    }

    @Override
    public CustomTaskMetric[] currentDatabaseMetrics() {
        final Snapshot snapshot = metricRegistry.histogram("mockRowTime").getSnapshot();
        return new CustomTaskMetric[] {
                new TaskMetric("ArchiveDatabaseRowCount", (long) metricRegistry.gauge("mockRowCount").getValue()),
                new TaskMetric("ArchiveDatabaseRowMaxLatency", snapshot.getMax()),
                new TaskMetric("ArchiveDatabaseRowAvgLatency", (long) snapshot.getMean()),
                new TaskMetric("ArchiveDatabaseRowMinLatency", snapshot.getMin()),
        };
    }

    @Override
    public Long getInitialOffset() {
        return archiveQuery.getInitialOffset();
    }
}
