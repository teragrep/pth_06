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

import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.jooq.types.ULong;

import java.sql.Connection;
import java.sql.Date;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class MockArchiveQueryProcessor implements ArchiveQuery {

    private final String query;
    private final SliceableTestDataSource sliceableTestDataSource;
    private final AtomicLong committedOffset;
    private final AtomicReference<Long> latestOffset;

    public MockArchiveQueryProcessor(final String query) {
        this(query, new SliceableMockDBRowSourceImpl(new MockDBRowSource()));
    }

    public MockArchiveQueryProcessor(final String query, final SliceableTestDataSource sliceableTestDataSource) {
        this.query = query;
        this.sliceableTestDataSource = sliceableTestDataSource;
        this.committedOffset = new AtomicLong(Long.MIN_VALUE);
        this.latestOffset = new AtomicReference<>();
    }

    @Override
    public Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> processBetweenUnixEpochHours(
            final long startHour,
            final long endHour
    ) {
        final long earliestAvailableStartHour = Math.max(committedOffset.get(), startHour);
        final PriorityQueue<MockDBRow> slice = sliceableTestDataSource.slice(earliestAvailableStartHour, endHour);

        final MockDataProvider provider = ctx -> {
            // use ordinary jooq api to create an org.jooq.result object.
            // you can also use ordinary jooq api to load csv files or
            // other formats, here!
            final DSLContext create = DSL.using(SQLDialect.DEFAULT);
            final Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result = create
                    .newResult(
                            SliceTable.id, SliceTable.directory, SliceTable.stream, SliceTable.host, SliceTable.logtag,
                            SliceTable.logdate, SliceTable.bucket, SliceTable.path, SliceTable.logtime,
                            SliceTable.filesize, SliceTable.uncompressedFilesize
                    );

            while (!slice.isEmpty()) {
                result.add(new RecordableMockDBRow(slice.poll()).asRecord());
            }

            return new MockResult[] {
                    new MockResult(result.size(), result)
            };
        };

        final Connection connection = new MockConnection(provider);
        final DSLContext create = DSL.using(connection, SQLDialect.DEFAULT);
        final Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result;
        try (
                final SelectSelectStep<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> step = create
                        .select(
                                SliceTable.id, SliceTable.directory, SliceTable.stream, SliceTable.host,
                                SliceTable.logtag, SliceTable.logdate, SliceTable.bucket, SliceTable.path,
                                SliceTable.logtime, SliceTable.filesize, SliceTable.uncompressedFilesize
                        )
        ) {
            result = step.fetch();
        }
        return result;
    }

    @Override
    public void commit(final long offset) {
        committedOffset.set(offset);
    }

    @Override
    public Long getInitialOffset() {
        final PriorityQueue<MockDBRow> queue = sliceableTestDataSource.asPriorityQueue();

        final Long initialOffset;
        if (!queue.isEmpty()) {
            final MockDBRow firstRow = queue.peek();
            initialOffset = firstRow.logtime();
        }
        else {
            initialOffset = null;
        }
        return initialOffset;
    }

    @Override
    public Long incrementAndGetLatestOffset() {
        final PriorityQueue<MockDBRow> queue = sliceableTestDataSource.asPriorityQueue();
        final Long newLatestOffset;

        if (queue.isEmpty()) {
            newLatestOffset = latestOffset.get();
        }
        else if (latestOffset.get() == null) {
            final MockDBRow firstRow = queue.peek();
            newLatestOffset = firstRow.logtime();
        }
        else {
            Long nextOffset = latestOffset.get();
            while (!queue.isEmpty()) {
                MockDBRow row = queue.poll();
                if (row.logtime() > latestOffset.get()) {
                    nextOffset = row.logtime();
                    break;
                }
            }
            newLatestOffset = nextOffset;
        }
        latestOffset.set(newLatestOffset);
        return newLatestOffset;
    }

    @Override
    public Long mostRecentOffset() {
        return latestOffset.get();
    }

    @Override
    public CustomTaskMetric[] currentDatabaseMetrics() {
        return new CustomTaskMetric[0];
    }
}
