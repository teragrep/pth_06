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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Objects;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.teragrep.pth_06.ConfiguredLogger;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.metrics.TaskMetric;
import com.teragrep.pth_06.planner.walker.ConditionWalker;
import com.teragrep.pth_06.planner.walker.FilterlessSearch;
import com.teragrep.pth_06.planner.walker.FilterlessSearchImpl;
import com.teragrep.pth_06.planner.walker.FilterlessSearchStub;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.jooq.*;
import org.jooq.conf.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;

import static org.jooq.impl.DSL.select;

// https://stackoverflow.com/questions/33657391/qualifying-a-temporary-table-column-name-in-jooq
// https://www.jooq.org/doc/latest/manual/sql-building/dynamic-sql/

/**
 * <h1>Stream DB Client</h1> Class for creating a streamdb client.
 *
 * @see <a
 *      href=https://stackoverflow.com/questions/33657391/qualifying-a-temporary-table-column-name-in-jooq>stackoverflow.com
 *      example</a>
 * @see <a href=https://www.jooq.org/doc/latest/manual/sql-building/dynamic-sql/>jooq.org dynamic sql manual</a>
 * @since 08/04/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 * @author Motoko Kusanagi
 * @author Ville Manninen
 */
public final class StreamDBClient implements AutoCloseable {

    private static final Logger classLogger = LoggerFactory.getLogger(StreamDBClient.class);
    private final ConfiguredLogger LOGGER;

    private final MetricRegistry metricRegistry;
    private final Connection connection;
    private final DSLContext ctx;
    private final long includeBeforeEpoch;
    private final boolean bloomEnabled;
    private final Condition journaldbCondition;
    private final ConditionWalker walker;
    private final boolean isDebugEnabled;
    private final boolean isLogSQL;
    private final GetArchivedObjectsFilterTable filterTable;
    private final NestedTopNQuery nestedTopNQuery;
    private final SliceTable sliceTable;

    public StreamDBClient(Config config) throws SQLException {
        this.isDebugEnabled = config.loggingConfig.isDebug();
        this.LOGGER = new ConfiguredLogger(classLogger, isDebugEnabled);
        this.isLogSQL = config.sqlConfig.isLog();
        LOGGER.debug("StreamDBClient ctor called with config <[{}]>", config);
        this.bloomEnabled = config.archiveConfig.bloomEnabled;
        LOGGER.info("StreamDBClient bloom.enabled: " + this.bloomEnabled);

        final String userName = config.archiveConfig.dbUsername;
        final String password = config.archiveConfig.dbPassword;
        final String url = config.archiveConfig.dbUrl;
        final String journaldbName = config.archiveConfig.dbJournalDbName;
        final String streamdbName = config.archiveConfig.dbStreamDbName;
        final String bloomdbName = config.archiveConfig.bloomDbName;
        final boolean isSQLThrowExceptionsNone = config.sqlConfig.isThrowExceptionsNone();
        final boolean isSQLExecuteLogging = config.sqlConfig.isExecuteLogging();
        final boolean withoutFilters = config.archiveConfig.withoutFilters;
        final String withoutFiltersPattern = config.archiveConfig.withoutFiltersPattern;
        // https://blog.jooq.org/how-i-incorrectly-fetched-jdbc-resultsets-again/
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(streamdbName), new MappedSchema().withInput("journaldb").withOutput(journaldbName), new MappedSchema().withInput("bloomdb").withOutput(bloomdbName)));
        if (isSQLThrowExceptionsNone) {
            settings = settings.withThrowExceptions(ThrowExceptions.THROW_NONE);
            LOGGER.warn("StreamDBClient SQL Exceptions set to THROW_NONE");
        }

        if (isSQLExecuteLogging) {
            settings.withExecuteLogging(true);
        }

        System.getProperties().setProperty("org.jooq.no-logo", "true");
        this.connection = DriverManager.getConnection(url, userName, password);
        this.ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        this.filterTable = new GetArchivedObjectsFilterTable(ctx, isDebugEnabled, isLogSQL);
        this.nestedTopNQuery = new NestedTopNQuery(this, isDebugEnabled);
        this.sliceTable = new SliceTable(ctx, isDebugEnabled, isLogSQL);

        if (isSQLThrowExceptionsNone) {
            // force sql mode to NO_ENGINE_SUBSTITUTION, STRICT mode

            final String noEngineSubstitution = "SET sql_mode = 'NO_ENGINE_SUBSTITUTION';";
            if (isLogSQL) {
                LOGGER
                        .info(
                                "{SQL} StreamDBClient noEngineSubstitution <\n{}\n>", "noEngineSubstitution",
                                noEngineSubstitution
                        );
            }
            ctx.execute(noEngineSubstitution);
        }

        // -- TODO use dslContext.batch for all initial operations
        final FilterlessSearch filterlessSearch;
        if (withoutFilters) {
            filterlessSearch = new FilterlessSearchImpl(ctx, withoutFiltersPattern);
        }
        else {
            filterlessSearch = new FilterlessSearchStub();
        }

        this.walker = new ConditionWalker(ctx, bloomEnabled, filterlessSearch);
        Condition streamdbCondition;

        try {
            // Construct both streamDB and journalDB query conditions
            streamdbCondition = walker.fromString(config.query, true);
            this.journaldbCondition = walker.fromString(config.query, false);
        }
        catch (ParserConfigurationException | IOException | SAXException e) {
            throw new IllegalArgumentException(e);
        }

        filterTable.create(streamdbCondition); // TEMPTABLE
        sliceTable.create();

        // by default no cutoff
        includeBeforeEpoch = config.archiveConfig.archiveIncludeBeforeEpoch;

        this.metricRegistry = new MetricRegistry();
        LOGGER.debug("StreamDBClient ctor exit");
    }

    public CustomTaskMetric[] currentDatabaseMetrics() {
        LOGGER.debug("StreamDBClient.currentDatabaseMetrics called");
        final Snapshot latencySnapshot = metricRegistry.histogram("ArchiveDatabaseLatencyPerRow").getSnapshot();
        return new CustomTaskMetric[] {
                new TaskMetric("ArchiveDatabaseRowCount", metricRegistry.counter("ArchiveDatabaseRowCount").getCount()),
                new TaskMetric("ArchiveDatabaseRowMaxLatency", latencySnapshot.getMax()),
                new TaskMetric("ArchiveDatabaseRowAvgLatency", (long) latencySnapshot.getMean()),
                new TaskMetric("ArchiveDatabaseRowMinLatency", latencySnapshot.getMin()),
        };
    }

    public int pullToSliceTable(Date day) {
        LOGGER.debug("StreamDBClient.pullToSliceTable called for date <{}>", day);

        SelectOnConditionStep<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> select = ctx
                .select(
                        JOURNALDB.LOGFILE.ID, nestedTopNQuery.directory(), nestedTopNQuery.stream(),
                        JOURNALDB.HOST.NAME, JOURNALDB.LOGFILE.LOGTAG, JOURNALDB.LOGFILE.LOGDATE, JOURNALDB.BUCKET.NAME,
                        JOURNALDB.LOGFILE.PATH, nestedTopNQuery.logtime(), JOURNALDB.LOGFILE.FILE_SIZE,
                        JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE
                )
                .from(nestedTopNQuery.getTableStatement(journaldbCondition, day))
                .join(JOURNALDB.LOGFILE)
                .on(JOURNALDB.LOGFILE.ID.eq(nestedTopNQuery.id()))
                .join(JOURNALDB.BUCKET)
                .on(JOURNALDB.BUCKET.ID.eq(JOURNALDB.LOGFILE.BUCKET_ID))
                .join(JOURNALDB.HOST)
                .on(JOURNALDB.HOST.ID.eq(JOURNALDB.LOGFILE.HOST_ID));

        final Timer.Context timerCtx = metricRegistry.timer("ArchiveDatabaseLatency").time();
        final int rows;

        try (final InsertOnDuplicateStep<Record> selectStep = ctx.insertInto(SliceTable.SLICE_TABLE).select(select)) {
            if (isLogSQL) {
                LOGGER
                        .info(
                                "{SQL} StreamDBClient.pullToSliceTable selectStep <\n{}\n>",
                                selectStep.getSQL(ParamType.INLINED)
                        );
            }
            rows = selectStep.execute();
        }

        final long latencyNs = timerCtx.stop();

        if (rows != 0) {
            metricRegistry.histogram("ArchiveDatabaseLatencyPerRow").update(latencyNs / rows);
        }

        LOGGER.info("StreamDBClient.pullToSliceTable" + ": took (" + "<{}> ms)", (latencyNs / 1_000_000d));

        metricRegistry.counter("ArchiveDatabaseRowCount").inc(rows);

        LOGGER.debug("StreamDBClient.pullToSliceTable returns <{}> rows", rows);
        return rows;

    }

    WeightedOffset getNextHourAndSizeFromSliceTable(long previousHour) {
        LOGGER.debug("StreamDBClient.getNextHourAndSizeFromSliceTable called with previousHour <{}>", previousHour);

        final Result<Record2<Long, BigDecimal>> hourAndFilesizeRecord = ctx
                .select(SliceTable.logtime, DSL.sum(SliceTable.filesize))
                .from(SliceTable.SLICE_TABLE)
                .where(SliceTable.logtime.greaterThan(previousHour).and(SliceTable.logtime.lessThan(includeBeforeEpoch))).groupBy(SliceTable.logtime).orderBy(SliceTable.logtime.asc()).limit(1).fetch();
        final WeightedOffset weightedOffset;
        if (hourAndFilesizeRecord.isEmpty()) {
            weightedOffset = new WeightedOffset();
        }
        else {
            final long offset = hourAndFilesizeRecord.get(0).get(0, Long.class);
            final long fileSize = hourAndFilesizeRecord.get(0).get(1, BigDecimal.class).longValue();

            weightedOffset = new WeightedOffset(offset, fileSize);
        }

        LOGGER.debug("StreamDBClient.getNextHourAndSizeFromSliceTable returns weightedOffset <{}>", weightedOffset);
        return weightedOffset;

    }

    void deleteRangeFromSliceTable(long start, long end) {
        LOGGER.debug("StreamDBClient.deleteRangeFromSliceTable called  start <{}> end <{}>", start, end);

        DeleteConditionStep<Record> deleteRangeStep = ctx
                .deleteFrom(SliceTable.SLICE_TABLE)
                .where(SliceTable.logtime.greaterThan(start).and(SliceTable.logtime.lessOrEqual(end)));

        if (isLogSQL) {
            LOGGER
                    .info(
                            "{SQL} StreamDBClient.deleteRangeFromSliceTable deleteRangeStep <\n{}\n>",
                            deleteRangeStep.getSQL(ParamType.INLINED)
                    );
        }
        deleteRangeStep.execute();

        LOGGER.debug("StreamDBClient.deleteRangeFromSliceTable exit");
    }

    Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> getHourRange(
            long excludedStartHour,
            long includedEndHour
    ) {
        LOGGER
                .debug(
                        "StreamDBClient.getHourRange called excludedStartHour <{}> includedEndHour <{}>",
                        excludedStartHour, includedEndHour
                );
        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result = ctx
                .select(
                        SliceTable.id, SliceTable.directory, SliceTable.stream, SliceTable.host, SliceTable.logtag,
                        SliceTable.logdate, SliceTable.bucket, SliceTable.path, SliceTable.logtime, SliceTable.filesize,
                        SliceTable.uncompressedFilesize
                )
                .from(SliceTable.SLICE_TABLE)
                .where(SliceTable.logtime.greaterThan(excludedStartHour).and(SliceTable.logtime.lessOrEqual(includedEndHour)).and(SliceTable.logtime.lessThan(includeBeforeEpoch))).fetch();

        LOGGER.debug("StreamDBClient.getHourRange returns <{}> records", result.size());
        return result;
    }

    ConditionWalker walker() {
        return this.walker;
    }

    boolean bloomEnabled() {
        return this.bloomEnabled;
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                LOGGER.debug("Closed JDBC connection");
            }
        }
        catch (final SQLException e) {
            LOGGER.warn("Error closing JDBC connection: <{}>", e.getMessage());
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamDBClient that = (StreamDBClient) o;
        return includeBeforeEpoch == that.includeBeforeEpoch
                && bloomEnabled == that.bloomEnabled && isDebugEnabled == that.isDebugEnabled && Objects
                        .equals(LOGGER, that.LOGGER)
                && Objects.equals(metricRegistry, that.metricRegistry) && Objects.equals(connection, that.connection) && Objects.equals(ctx, that.ctx) && Objects.equals(journaldbCondition, that.journaldbCondition) && Objects.equals(walker, that.walker) && Objects.equals(filterTable, that.filterTable) && Objects.equals(sliceTable, that.sliceTable) && isLogSQL == that.isLogSQL;
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(
                        LOGGER, metricRegistry, connection, ctx, includeBeforeEpoch, bloomEnabled, journaldbCondition,
                        walker, isDebugEnabled, filterTable, sliceTable, isLogSQL
                );
    }
}
