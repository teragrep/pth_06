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
import com.teragrep.pth_06.jooq.generated.journaldb.Indexes;
import com.teragrep.pth_06.planner.bloomfilter.ConditionMatchBloomDBTables;
import com.teragrep.pth_06.planner.walker.conditions.QueryCondition;
import com.teragrep.pth_06.planner.walker.conditions.StringEqualsCondition;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;

public final class NestedTopNQuery {

    private static final Logger classLogger = LoggerFactory.getLogger(NestedTopNQuery.class);
    private final ConfiguredLogger logger;
    private final StreamDBClient streamDBClient;
    private final boolean excludePatternEnabled;
    private final String excludePattern;
    private final String innerTableName = "limited";
    private final Table<Record> innerTable = DSL.table(DSL.name(innerTableName));

    // TODO refactor: heavily database session dependant: create synthetic logtime field, based on the path
    public final Field<Long> logtimeFunction = DSL
            .field(
                    "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR({0},'^\\\\d{4}\\\\/\\\\d{2}-\\\\d{2}\\\\/[\\\\w\\\\.-]+\\\\/([^\\\\p{Z}\\\\p{C}]+?)\\\\/([^\\\\p{Z}\\\\p{C}]+)(-@)?(\\\\d+|)-(\\\\d{4}\\\\d{2}\\\\d{2}\\\\d{2})'), -10, 10), '%Y%m%d%H'))",
                    Long.class, JOURNALDB.LOGFILE.PATH
            );

    private final Field<ULong> id = DSL.field(DSL.name(innerTableName, "id"), ULong.class);
    private final Field<String> directory = DSL.field(DSL.name(innerTableName, "directory"), String.class);
    private final Field<String> stream = DSL.field(DSL.name(innerTableName, "stream"), String.class);
    private final Field<Long> logtime = DSL.field(DSL.name(innerTableName, "logtime"), Long.class);
    private final Field<Long> logtimeForOrderBy = DSL.field("logtime", Long.class);

    private final SelectField<?>[] resultFields = {
            JOURNALDB.LOGFILE.ID.as(id),
            GetArchivedObjectsFilterTable.directory.as(directory),
            GetArchivedObjectsFilterTable.stream.as(stream),
            logtimeFunction.as(logtime)
    };

    public NestedTopNQuery(final StreamDBClient streamDBClient, final boolean isDebug) {
        this(streamDBClient, new ConfiguredLogger(classLogger, isDebug), false, "");

    }

    public NestedTopNQuery(
            final StreamDBClient streamDBClient,
            final boolean isDebug,
            final boolean excludePatternEnabled,
            final String excludePattern
    ) {
        this(streamDBClient, new ConfiguredLogger(classLogger, isDebug), excludePatternEnabled, excludePattern);

    }

    public NestedTopNQuery(
            final StreamDBClient streamDBClient,
            final ConfiguredLogger configuredLogger,
            final boolean excludePatternEnabled,
            final String excludePattern
    ) {
        this.streamDBClient = streamDBClient;
        this.logger = configuredLogger;
        this.excludePatternEnabled = excludePatternEnabled;
        this.excludePattern = excludePattern;
    }

    public Table<Record> getTableStatement(Condition journaldbConditionArg, Date day) {
        logger.debug("NestedTopNQuery.getTableStatement called condition <{}> date <{}>", journaldbConditionArg, day);
        SelectOnConditionStep<Record> selectOnConditionStep = DSL
                .select(resultFields)
                .from(GetArchivedObjectsFilterTable.FILTER_TABLE)
                .innerJoin(JOURNALDB.LOGFILE.forceIndex(Indexes.LOGFILE_CIX_LOGFILE_LOGDATE_HOST_ID_LOGTAG.getName()))
                .on(JOURNALDB.LOGFILE.HOST_ID.eq(GetArchivedObjectsFilterTable.host_id).and(JOURNALDB.LOGFILE.LOGTAG.eq(GetArchivedObjectsFilterTable.tag)));

        if (excludePatternEnabled && streamDBClient.bloomEnabled()) {
            final QueryCondition stringEqualsCondition = new StringEqualsCondition(
                    excludePattern,
                    BLOOMDB.FILTERTYPE.PATTERN
            );
            final ConditionMatchBloomDBTables matchBloomDBTables = new ConditionMatchBloomDBTables(
                    streamDBClient.ctx(),
                    stringEqualsCondition
            );
            // join all tables that match the pattern to be excluded, so they can be filtered
            for (final Table<?> table : matchBloomDBTables.tables()) {
                selectOnConditionStep = selectOnConditionStep
                        .leftJoin(table)
                        .on(JOURNALDB.LOGFILE.ID.eq(table.field("partition_id", ULong.class)))
                        .leftJoin(BLOOMDB.FILTERTYPE)
                        .on(BLOOMDB.FILTERTYPE.ID.eq(table.field("filter_type_id", ULong.class)).and(BLOOMDB.FILTERTYPE.PATTERN.eq(excludePattern)));
                journaldbConditionArg = journaldbConditionArg.and(BLOOMDB.FILTERTYPE.ID.isNull());
            }
        }
        else if (streamDBClient.bloomEnabled()) {
            // join all tables needed for the condition generated by walker
            final Set<Table<?>> tables = streamDBClient.walker().conditionRequiredTables();
            if (!tables.isEmpty()) {
                for (final Table<?> table : tables) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Left join pattern match table: <{}>", table.getName());
                    }
                    selectOnConditionStep = selectOnConditionStep
                            .leftJoin(table)
                            .on(JOURNALDB.LOGFILE.ID.eq((Field<ULong>) table.field("partition_id")));
                }
            }
        }

        logger.debug("NestedTopNQuery.getTableStatement exit");
        return selectOnConditionStep
                .where(JOURNALDB.LOGFILE.LOGDATE.eq(day).and(journaldbConditionArg))
                .orderBy(logtimeForOrderBy, JOURNALDB.LOGFILE.ID.asc())
                .asTable(innerTable);
    }

    public Field<Long> logtime() {
        return logtime;
    }

    public Field<String> stream() {
        return stream;
    }

    public Field<String> directory() {
        return directory;
    }

    public Field<ULong> id() {
        return id;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NestedTopNQuery that = (NestedTopNQuery) o;
        return Objects.equals(logger, that.logger) && Objects.equals(streamDBClient, that.streamDBClient)
                && excludePatternEnabled == that.excludePatternEnabled && Objects
                        .equals(excludePattern, that.excludePattern)
                && Objects.equals(innerTableName, that.innerTableName) && Objects.equals(innerTable, that.innerTable) && Objects.equals(logtimeFunction, that.logtimeFunction) && Objects.equals(id, that.id) && Objects.equals(directory, that.directory) && Objects.equals(stream, that.stream) && Objects.equals(logtime, that.logtime) && Objects.equals(logtimeForOrderBy, that.logtimeForOrderBy) && Objects.deepEquals(resultFields, that.resultFields);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(
                        logger, streamDBClient, excludePatternEnabled, excludePattern, innerTableName, innerTable,
                        logtimeFunction, id, directory, stream, logtime, logtimeForOrderBy, Arrays.hashCode(resultFields)
                );
    }
}
