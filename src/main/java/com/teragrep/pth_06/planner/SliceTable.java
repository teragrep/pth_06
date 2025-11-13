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

import com.teragrep.pth_06.ConfiguredLogger;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;

public final class SliceTable {

    private static final String sliceTableName = "sliceTable";
    private final DSLContext ctx;
    private static final Logger classLogger = LoggerFactory.getLogger(SliceTable.class);
    private final ConfiguredLogger LOGGER;
    public static final Table<Record> SLICE_TABLE = DSL.table(DSL.name(sliceTableName));
    public static final Field<ULong> id = DSL.field(DSL.name(sliceTableName, "id"), ULong.class);
    public static final Field<String> directory = DSL.field(DSL.name(sliceTableName, "directory"), String.class);
    public static final Field<String> stream = DSL.field(DSL.name(sliceTableName, "stream"), String.class);
    public static final Field<String> host = DSL.field(DSL.name(sliceTableName, "host"), String.class);
    public static final Field<String> logtag = DSL.field(DSL.name(sliceTableName, "logtag"), String.class);
    public static final Field<Date> logdate = DSL.field(DSL.name(sliceTableName, "logdate"), Date.class);
    public static final Field<String> bucket = DSL.field(DSL.name(sliceTableName, "bucket"), String.class);
    public static final Field<String> path = DSL.field(DSL.name(sliceTableName, "path"), String.class);
    public static final Field<Long> logtime = DSL.field(DSL.name(sliceTableName, "logtime"), Long.class);
    public static final Field<ULong> filesize = DSL.field(DSL.name(sliceTableName, "filesize"), ULong.class);
    // additional metadata
    public static final Field<ULong> uncompressedFilesize = DSL
            .field(DSL.name(sliceTableName, "uncompressed_filesize"), ULong.class);

    private static final Index logtimeIndex = DSL.index(DSL.name("ix_logtime"));

    public SliceTable(final DSLContext ctx, final boolean isDebugEnabled) {
        this.ctx = ctx;
        this.LOGGER = new ConfiguredLogger(classLogger, isDebugEnabled);
    }

    public void create() {
        LOGGER.debug("SliceTable.create called");
        try (final DropTableStep dropTableStep = ctx.dropTemporaryTableIfExists(SLICE_TABLE)) {
            dropTableStep.execute();
        }
        try (
                final CreateTableColumnStep createTableStep = ctx.createTemporaryTable(SLICE_TABLE).columns(id, directory, stream, host, logtag, logdate, bucket, path, logtime, filesize, uncompressedFilesize)
        ) {
            createTableStep.execute();
        }
        try (final CreateIndexIncludeStep createIndexStep = ctx.createIndex(logtimeIndex).on(SLICE_TABLE, logtime)) {
            createIndexStep.execute();
        }
        LOGGER.debug("SliceTable.create exit");
    }

}
