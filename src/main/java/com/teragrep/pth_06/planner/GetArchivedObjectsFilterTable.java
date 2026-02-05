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
import com.teragrep.pth_06.config.ArchiveConfig;
import com.teragrep.pth_06.planner.bloomfilter.ConditionMatchBloomDBTables;
import com.teragrep.pth_06.planner.walker.conditions.QueryCondition;
import com.teragrep.pth_06.planner.walker.conditions.StringEqualsCondition;
import com.teragrep.pth_06.planner.walker.conditions.WithoutPatternCondition;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.pth_06.jooq.generated.streamdb.Streamdb.STREAMDB;

public final class GetArchivedObjectsFilterTable {

    // temporary table created from streamdb
    private static final String tmpTableName = "getArchivedObjects_filter_table";
    public static final Table<Record> FILTER_TABLE = DSL.table(DSL.name(tmpTableName));
    public static final Field<UShort> host_id = DSL.field(DSL.name(tmpTableName, "host_id"), UShort.class);
    public static final Field<String> host = DSL.field(DSL.name(tmpTableName, "host"), String.class);
    public static final Field<String> tag = DSL.field(DSL.name(tmpTableName, "tag"), String.class);
    public static final Field<String> directory = DSL.field(DSL.name(tmpTableName, "directory"), String.class);
    public static final Field<String> stream = DSL.field(DSL.name(tmpTableName, "stream"), String.class);
    private static final Index hostIndex = DSL.index(DSL.name("cix_host_id_tag"));

    private final ConfiguredLogger logger;
    private static final Logger classLogger = LoggerFactory.getLogger(GetArchivedObjectsFilterTable.class);
    private final DSLContext ctx;
    private final boolean excludePatternMode;
    private final String excludePattern;

    private final boolean isLogSQL;

    public GetArchivedObjectsFilterTable(
            final DSLContext ctx,
            final boolean isDebug,
            final boolean isLogSQL,
            final boolean excludePatternMode,
            final String excludePattern
    ) {
        this.ctx = ctx;
        this.logger = new ConfiguredLogger(classLogger, isDebug);
        this.isLogSQL = isLogSQL;
        this.excludePatternMode = excludePatternMode;
        this.excludePattern = excludePattern;
    }

    public void create(final Condition streamdbCondition) {
        logger.debug("GetArchivedObjectsFilterTable.create called condition <{}>", streamdbCondition);
        DropTableStep dropQuery = ctx.dropTemporaryTableIfExists(GetArchivedObjectsFilterTable.FILTER_TABLE);

        if (isLogSQL) {
            logger
                    .info(
                            "{SQL} GetArchivedObjectsFilterTable.create dropQuery <\n{}\n>",
                            dropQuery.getSQL(ParamType.INLINED)
                    );
        }

        dropQuery.execute();

        SelectOnConditionStep<Record> selectOnConditionStep = ctx
                .select(
                        // these are hardcoded for the procedure execution
                        STREAMDB.STREAM.DIRECTORY.as(GetArchivedObjectsFilterTable.directory)
                )
                .select(STREAMDB.STREAM.STREAM_.as(GetArchivedObjectsFilterTable.stream))
                .select(STREAMDB.STREAM.TAG.as(GetArchivedObjectsFilterTable.tag))
                .select((JOURNALDB.HOST.NAME.as(GetArchivedObjectsFilterTable.host)))
                .select((JOURNALDB.HOST.ID.as(GetArchivedObjectsFilterTable.host_id)))
                .from(STREAMDB.STREAM)
                .innerJoin(STREAMDB.LOG_GROUP)
                .on((STREAMDB.STREAM.GID).eq(STREAMDB.LOG_GROUP.ID))
                .innerJoin(STREAMDB.HOST)
                .on((STREAMDB.HOST.GID).eq(STREAMDB.LOG_GROUP.ID))
                .innerJoin(JOURNALDB.HOST)
                .on((STREAMDB.HOST.NAME).eq(JOURNALDB.HOST.NAME));


        final SelectConditionStep<Record> selectStep;
        if (excludePatternMode) {
            if (isLogSQL) {
                logger.info("Using exclude matching pattern mode with pattern <[{}]>", excludePattern);
            }
            if (excludePattern.isEmpty()) {
                throw new IllegalArgumentException(
                        "without filters pattern <[" + excludePattern
                                + "]> was empty when using exclude matching pattern mode"
                );
            }
            final QueryCondition patternEqualsCondition = new StringEqualsCondition(
                    excludePattern,
                    BLOOMDB.FILTERTYPE.PATTERN
            );
            final ConditionMatchBloomDBTables conditionMatchBloomDBTables = new ConditionMatchBloomDBTables(
                    ctx,
                    patternEqualsCondition
            );
            for (Table<?> table : conditionMatchBloomDBTables.tables()) {
                selectOnConditionStep = selectOnConditionStep
                        .leftJoin(table)
                        .on(JOURNALDB.LOGFILE.ID.eq(table.field("logfile_id", ULong.class)))
                        .leftJoin(BLOOMDB.FILTERTYPE)
                        .on(BLOOMDB.FILTERTYPE.ID.eq(table.field("filtertype_id", ULong.class)));
            }
            WithoutPatternCondition withoutPatternCondition = new WithoutPatternCondition(ctx, excludePattern);
            selectStep = selectOnConditionStep.where(streamdbCondition.and(withoutPatternCondition.condition()));
        }
        else {
            selectStep = selectOnConditionStep.where(streamdbCondition);
        }

        CreateTableWithDataStep query = ctx
                .createTemporaryTable(GetArchivedObjectsFilterTable.FILTER_TABLE)
                .as(selectStep);

        if (isLogSQL) {
            logger.info("{SQL} GetArchivedObjectsFilterTable.create query <\n{}\n>", query.getSQL(ParamType.INLINED));
        }

        query.execute();

        // this could be within tmpTableCreateSql but JOOQ can't (yet) https://github.com/jOOQ/jOOQ/issues/11752
        try (
                final CreateIndexIncludeStep indexStep = ctx.createIndex(GetArchivedObjectsFilterTable.hostIndex)
                        //.on(FILTER_TABLE, directory, host_id, tag, stream).execute(); // FIXME this happens only on dev kube due to old mariadb: Index column size too large. The maximum column size is 767 bytes.
                        .on(
                                GetArchivedObjectsFilterTable.FILTER_TABLE, GetArchivedObjectsFilterTable.host_id,
                                GetArchivedObjectsFilterTable.tag
                        )
        ) {
            if (isLogSQL) {
                logger
                        .info(
                                "{SQL} GetArchivedObjectsFilterTable.create indexStep <\n{}\n>",
                                indexStep.getSQL(ParamType.INLINED)
                        );
            }
            indexStep.execute();
        }
        logger.debug("GetArchivedObjectsFilterTable.create exit");
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GetArchivedObjectsFilterTable that = (GetArchivedObjectsFilterTable) o;
        return Objects.equals(logger, that.logger) && Objects.equals(ctx, that.ctx) && isLogSQL == that.isLogSQL;
    }

    @Override
    public int hashCode() {
        return Objects.hash(logger, ctx, isLogSQL);
    }
}
