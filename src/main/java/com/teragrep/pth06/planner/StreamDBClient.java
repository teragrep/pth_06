/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

package com.teragrep.pth06.planner;

import java.sql.*;
import java.time.Instant;

import org.jooq.*;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.conf.ThrowExceptions;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.teragrep.pth06.jooq.generated.streamdb.Streamdb.STREAMDB;
import static com.teragrep.pth06.jooq.generated.journaldb.Journaldb.JOURNALDB;

import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.select;

// https://stackoverflow.com/questions/33657391/qualifying-a-temporary-table-column-name-in-jooq
// https://www.jooq.org/doc/latest/manual/sql-building/dynamic-sql/

/**
 * <h2>Stream DB Client</h2>
 *
 * Class for creating a streamdb client.
 *
 * @see <a href=https://stackoverflow.com/questions/33657391/qualifying-a-temporary-table-column-name-in-jooq>stackoverflow.com example</a>
 * @see <a href=https://www.jooq.org/doc/latest/manual/sql-building/dynamic-sql/>jooq.org dynamic sql manual</a>
 * @since 08/04/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 * @author Motoko Kusanagi
 */
public class StreamDBClient {
    private final Logger LOGGER = LoggerFactory.getLogger(StreamDBClient.class);

    private final DSLContext ctx;
    private final Connection connection;
    private long includeBeforeEpoch;

    public StreamDBClient(String userName, String password, String url,
                          String journaldbName, String streamdbName,
                          Condition streamdbCondition,
                          boolean hideDatabaseExceptions) throws SQLException {

        // https://blog.jooq.org/how-i-incorrectly-fetched-jdbc-resultsets-again/
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping()
                        .withSchemata(
                                new MappedSchema().withInput("streamdb").withOutput(streamdbName),
                                new MappedSchema().withInput("journaldb").withOutput(journaldbName)
                        ));
        if (hideDatabaseExceptions) {
                settings =
                        settings.withThrowExceptions(ThrowExceptions.THROW_NONE);
        }

        System.getProperties().setProperty("org.jooq.no-logo", "true");
        this.connection = DriverManager.getConnection(url, userName, password);
        this.ctx = DSL.using(connection, SQLDialect.MYSQL, settings);

        if (hideDatabaseExceptions) {
            // force sql mode to NO_ENGINE_SUBSTITUTION, STRICT mode
            this.ctx.execute("SET sql_mode = 'NO_ENGINE_SUBSTITUTION';");
        }

        // -- TODO use dslContext.batch for all initial operations
        GetArchivedObjectsFilterTable.create(this.ctx, streamdbCondition); // TEMPTABLE
        SliceTable.create(this.ctx);

        // by default no cutoff
        includeBeforeEpoch = Long.MAX_VALUE;
    }

    public void setIncludeBeforeEpoch(long includeBeforeEpoch) {
        this.includeBeforeEpoch = includeBeforeEpoch;
    }

    public int pullToSliceTable(Condition journaldbCondition, Date day) {

        SelectOnConditionStep<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> select =
                ctx.select(
                                JOURNALDB.LOGFILE.ID,
                                NestedTopNQuery.directory,
                                NestedTopNQuery.stream,
                                JOURNALDB.HOST.NAME,
                                JOURNALDB.LOGFILE.LOGTAG,
                                JOURNALDB.LOGFILE.LOGDATE,
                                JOURNALDB.BUCKET.NAME,
                                JOURNALDB.LOGFILE.PATH,
                                NestedTopNQuery.logtime,
                                JOURNALDB.LOGFILE.FILE_SIZE
                        )
                        .from(NestedTopNQuery.getTableStatement(journaldbCondition, day))
                        .join(JOURNALDB.LOGFILE).on(JOURNALDB.LOGFILE.ID.eq(NestedTopNQuery.id))
                        .join(JOURNALDB.BUCKET).on(JOURNALDB.BUCKET.ID.eq(JOURNALDB.LOGFILE.BUCKET_ID))
                        .join(JOURNALDB.HOST).on(JOURNALDB.HOST.ID.eq(JOURNALDB.LOGFILE.HOST_ID));

        final Instant stopwatch = Instant.now();
        int rows = ctx.insertInto(SliceTable.SLICE_TABLE).select(select).execute();

        LOGGER.info("StreamDBClient.pullToSliceTable" + ": took (" + (Instant.now().toEpochMilli() - stopwatch.toEpochMilli()) + "ms)");
        return rows;

    }

    public Result<Record1<ULong>> getEarliestHourFromSliceTable() {
        return ctx.selectDistinct(SliceTable.logtime)
                .from(SliceTable.SLICE_TABLE)
                .where(
                        SliceTable.logtime.lessThan(ULong.valueOf(includeBeforeEpoch))
                )
                .orderBy(SliceTable.logtime.asc())
                .limit(1)
                .fetch();
    }

    public Result<Record1<ULong>> getNextHourFromSliceTable(long previousHour) {
        return ctx.selectDistinct(SliceTable.logtime)
                .from(SliceTable.SLICE_TABLE)
                .where(
                        SliceTable.logtime.greaterThan(ULong.valueOf(previousHour))
                                .and(
                                        SliceTable.logtime.lessThan(ULong.valueOf(includeBeforeEpoch))
                                )
                )
                .orderBy(SliceTable.logtime.asc())
                .limit(1)
                .fetch();
    }

    public void deleteRangeFromSliceTable(long start, long end) {
        ctx.deleteFrom(SliceTable.SLICE_TABLE)
                .where(
                        SliceTable.logtime.between(ULong.valueOf(start), ULong.valueOf(end))
                )
                .execute();
    }

    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> getHour(long hour) {
        return ctx.select(
                        SliceTable.id,
                        SliceTable.directory,
                        SliceTable.stream,
                        SliceTable.host,
                        SliceTable.logtag,
                        SliceTable.logdate,
                        SliceTable.bucket,
                        SliceTable.path,
                        SliceTable.logtime,
                        SliceTable.filesize
                )
                .from(SliceTable.SLICE_TABLE)
                .where(SliceTable.logtime.eq(ULong.valueOf(hour))
                        .and(
                                SliceTable.logtime.lessThan(ULong.valueOf(includeBeforeEpoch))
                        )
                )
                .fetch();
    }

    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> getHourRange(long excludedStartHour, long includedEndHour) {
        return ctx.select(
                SliceTable.id,
                SliceTable.directory,
                SliceTable.stream,
                SliceTable.host,
                SliceTable.logtag,
                SliceTable.logdate,
                SliceTable.bucket,
                SliceTable.path,
                SliceTable.logtime,
                SliceTable.filesize
        )
                .from(SliceTable.SLICE_TABLE)
                .where(
                        SliceTable.logtime.greaterThan(ULong.valueOf(excludedStartHour))
                                .and(
                                        SliceTable.logtime.lessOrEqual(ULong.valueOf(includedEndHour))
                                )
                                .and(
                                        SliceTable.logtime.lessThan(ULong.valueOf(includeBeforeEpoch))
                                )
                )
                .fetch();
    }

    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> getHoursBeforeFromSliceTable(long includedEndHour) {
        return ctx.select(
                        SliceTable.id,
                        SliceTable.directory,
                        SliceTable.stream,
                        SliceTable.host,
                        SliceTable.logtag,
                        SliceTable.logdate,
                        SliceTable.bucket,
                        SliceTable.path,
                        SliceTable.logtime,
                        SliceTable.filesize
                )
                .from(SliceTable.SLICE_TABLE)
                .where(
                        SliceTable.logtime.lessOrEqual(ULong.valueOf(includedEndHour))
                                .and(
                                        SliceTable.logtime.lessThan(ULong.valueOf(includeBeforeEpoch))
                                )
                )
                .fetch();
    }

    public static class SliceTable {


        private static final String sliceTableName = "sliceTable";

        public static final Table<Record> SLICE_TABLE = DSL.table(DSL.name(sliceTableName));
        public static final Field<ULong> id = DSL.field(DSL.name(sliceTableName, "id"), ULong.class);
        public static final Field<String> directory = DSL.field(DSL.name(sliceTableName, "directory"), String.class);
        public static final Field<String> stream = DSL.field(DSL.name(sliceTableName, "stream"), String.class);
        public static final Field<String> host = DSL.field(DSL.name(sliceTableName, "host"), String.class);
        public static final Field<String> logtag = DSL.field(DSL.name(sliceTableName, "logtag"), String.class);
        public static final Field<Date> logdate = DSL.field(DSL.name(sliceTableName, "logdate"), Date.class);
        public static final Field<String> bucket = DSL.field(DSL.name(sliceTableName, "bucket"), String.class);
        public static final Field<String> path = DSL.field(DSL.name(sliceTableName, "path"), String.class);
        public static final Field<ULong> logtime = DSL.field(DSL.name(sliceTableName, "logtime"), ULong.class);
        public static final Field<ULong> filesize = DSL.field(DSL.name(sliceTableName, "filesize"), ULong.class);

        private static final Index logtimeIndex = DSL.index(DSL.name("ix_logtime"));

        private static void create(DSLContext ctx) {
            DropTableStep dropQuery = ctx.dropTemporaryTableIfExists(SLICE_TABLE);
            dropQuery.execute();
            CreateTableColumnStep query = ctx.createTemporaryTable(SLICE_TABLE)
                    .columns(
                            id,
                            directory,
                            stream,
                            host,
                            logtag,
                            logdate,
                            bucket,
                            path,
                            logtime,
                            filesize
                    );
            query.execute();

            ctx.createIndex(logtimeIndex).on(SLICE_TABLE, logtime).execute();
        }

    }

    public static class GetArchivedObjectsFilterTable {
        // temporary table created from streamdb

        private static final String tmpTableName = "getArchivedObjects_filter_table";

        private static final Table<Record> FILTER_TABLE = DSL.table(DSL.name(tmpTableName));
        public static final Field<UShort> host_id = DSL.field(DSL.name(tmpTableName, "host_id"), UShort.class);
        public static final Field<String> host = DSL.field(DSL.name(tmpTableName, "host"), String.class);
        private static final Field<String> tag = DSL.field(DSL.name(tmpTableName, "tag"), String.class);
        public static final Field<String> directory = DSL.field(DSL.name(tmpTableName, "directory"), String.class);
        public static final Field<String> stream = DSL.field(DSL.name(tmpTableName, "stream"), String.class);

        private static final Index hostIndex = DSL.index(DSL.name("cix_host_id_tag"));

        private static void create(DSLContext ctx, Condition streamdbCondition) {
            DropTableStep dropQuery = ctx.dropTemporaryTableIfExists(GetArchivedObjectsFilterTable.FILTER_TABLE);
            dropQuery.execute();

            CreateTableWithDataStep query = ctx.createTemporaryTable(GetArchivedObjectsFilterTable.FILTER_TABLE)
                    .as(
                            select(
                                    // these are hardcoded for the procedure execution
                                    STREAMDB.STREAM.DIRECTORY.as(GetArchivedObjectsFilterTable.directory))
                                    .select(STREAMDB.STREAM.STREAM_.as(GetArchivedObjectsFilterTable.stream))
                                    .select(STREAMDB.STREAM.TAG.as(GetArchivedObjectsFilterTable.tag))
                                    .select((JOURNALDB.HOST.NAME.as(GetArchivedObjectsFilterTable.host)))
                                    .select((JOURNALDB.HOST.ID.as(GetArchivedObjectsFilterTable.host_id)))
                                    .from(STREAMDB.STREAM)
                                    .innerJoin(STREAMDB.LOG_GROUP).on((STREAMDB.STREAM.GID).eq(STREAMDB.LOG_GROUP.ID))
                                    .innerJoin(STREAMDB.HOST).on((STREAMDB.HOST.GID).eq(STREAMDB.LOG_GROUP.ID))
                                    .innerJoin(JOURNALDB.HOST).on((STREAMDB.HOST.NAME).eq(JOURNALDB.HOST.NAME))
                                    // following change
                                    .where(streamdbCondition)
                    );
            query.execute();


            // this could be within tmpTableCreateSql but JOOQ can't (yet) https://github.com/jOOQ/jOOQ/issues/11752
            ctx.createIndex(GetArchivedObjectsFilterTable.hostIndex)
                    //.on(FILTER_TABLE, directory, host_id, tag, stream).execute(); // FIXME this happens only on dev kube due to old mariadb: Index column size too large. The maximum column size is 767 bytes.
                    .on(GetArchivedObjectsFilterTable.FILTER_TABLE, GetArchivedObjectsFilterTable.host_id, GetArchivedObjectsFilterTable.tag).execute();
        }

    }

    public static class NestedTopNQuery {

        private static final String innerTableName = "limited";
        private static final Table<Record> innerTable = DSL.table(DSL.name(innerTableName));

        // TODO refactor: heavily database session dependant: create synthetic logtime field, based on the path
        public static final Field<ULong> logtimeFunction = DSL.field(
                        "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR({0},'^\\\\d{4}\\\\/\\\\d{2}-\\\\d{2}\\\\/[\\\\w\\\\.-]+\\\\/([^\\\\p{Z}\\\\p{C}]+?)\\\\/([^\\\\p{Z}\\\\p{C}]+)(-@)?(\\\\d+|)-(\\\\d{4}\\\\d{2}\\\\d{2}\\\\d{2})'), -10, 10), '%Y%m%d%H'))",
                        ULong.class,
                        JOURNALDB.LOGFILE.PATH);

        private static final Field<ULong> id = DSL.field(DSL.name(innerTableName, "id"), ULong.class);
        private static final Field<String> directory = DSL.field(DSL.name(innerTableName, "directory"), String.class);
        private static final Field<String> stream = DSL.field(DSL.name(innerTableName, "stream"), String.class);
        private static final Field<ULong> logtime = DSL.field(DSL.name(innerTableName, "logtime"), ULong.class);

        private static final Field<ULong> logtimeForOrderBy = DSL.field("logtime", ULong.class);

        private static final SelectField<?>[] resultFields = {
                JOURNALDB.LOGFILE.ID.as(id),
                GetArchivedObjectsFilterTable.directory.as(directory),
                GetArchivedObjectsFilterTable.stream.as(stream),
                logtimeFunction.as(logtime)
        };

        private static Table<Record> getTableStatement(Condition journaldbCondition, Date day) {
            return DSL.select(resultFields).from(GetArchivedObjectsFilterTable.FILTER_TABLE)
                    .innerJoin(
                            JOURNALDB.LOGFILE).on(JOURNALDB.LOGFILE.HOST_ID.eq(GetArchivedObjectsFilterTable.host_id)
                            .and(
                                    JOURNALDB.LOGFILE.LOGTAG.eq(GetArchivedObjectsFilterTable.tag)
                            )
                    )
                    .where(JOURNALDB.LOGFILE.LOGDATE.eq(day)
                            .and(journaldbCondition)
                    )
                    .orderBy(logtimeForOrderBy, JOURNALDB.LOGFILE.ID.asc())
                    //.limit(numberOfRows)
                    .asTable(innerTable);
        }

    }
}
