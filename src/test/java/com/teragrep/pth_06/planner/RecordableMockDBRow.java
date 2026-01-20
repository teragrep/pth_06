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

import org.jooq.DSLContext;
import org.jooq.Record11;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import java.sql.Date;

public final class RecordableMockDBRow implements MockDBRow, Recordable {

    private final DSLContext dslContext;
    private final MockDBRow origin;

    public RecordableMockDBRow(final MockDBRow origin) {
        this(DSL.using(SQLDialect.DEFAULT), origin);
    }

    public RecordableMockDBRow(final DSLContext dslContext, final MockDBRow origin) {
        this.dslContext = dslContext;
        this.origin = origin;
    }

    @Override
    public long id() {
        return origin.id();
    }

    @Override
    public String directory() {
        return origin.directory();
    }

    @Override
    public String stream() {
        return origin.stream();
    }

    @Override
    public String host() {
        return origin.host();
    }

    @Override
    public String logtag() {
        return origin.logtag();
    }

    @Override
    public Date logdate() {
        return origin.logdate();
    }

    @Override
    public String bucket() {
        return origin.bucket();
    }

    @Override
    public String path() {
        return origin.path();
    }

    @Override
    public long logtime() {
        return origin.logtime();
    }

    @Override
    public long filesize() {
        return origin.filesize();
    }

    @Override
    public Long uncompressedFilesize() {
        return origin.uncompressedFilesize();
    }

    @Override
    public Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> asRecord() {
        final Record11<ULong, String, String, String, String, java.sql.Date, String, String, Long, ULong, ULong> newRecord = dslContext
                .newRecord(
                        SliceTable.id, SliceTable.directory, SliceTable.stream, SliceTable.host, SliceTable.logtag,
                        SliceTable.logdate, SliceTable.bucket, SliceTable.path, SliceTable.logtime, SliceTable.filesize,
                        SliceTable.uncompressedFilesize
                );

        newRecord.set(SliceTable.id, ULong.valueOf(origin.id()));
        newRecord.set(SliceTable.directory, origin.directory());
        newRecord.set(SliceTable.stream, origin.stream());
        newRecord.set(SliceTable.host, origin.host());
        newRecord.set(SliceTable.logtag, origin.logtag());
        newRecord.set(SliceTable.logdate, origin.logdate());
        newRecord.set(SliceTable.bucket, origin.bucket());
        newRecord.set(SliceTable.path, origin.path());
        newRecord.set(SliceTable.logtime, origin.logtime());
        newRecord.set(SliceTable.filesize, ULong.valueOf(origin.filesize()));
        newRecord
                .set(
                        SliceTable.uncompressedFilesize,
                        origin.uncompressedFilesize() != null ? ULong.valueOf(origin.uncompressedFilesize()) : null
                );

        return newRecord;
    }

    @Override
    public int compareTo(final MockDBRow o) {
        return origin.compareTo(o);
    }
}
