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

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.records.LogfileRecord;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.*;
import java.time.*;
import java.util.HashMap;
import java.util.Map;

import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;

class StreamDBClientTest {

    private MariaDBContainer<?> mariadb;
    private Connection connection;
    private ZoneId zoneId = ZoneId.of("America/New_York");
    private final String streamDBUsername = "streamdb";
    private final String streamDBPassword = "streamdb_pass";

    private final String streamdbName = "streamdb";
    private final String journaldbName = "journaldb";
    private final Map<String, String> opts = new HashMap<String, String>() {

        {
            put("S3endPoint", "mock");
            put("S3identity", "mock");
            put("S3credential", "mock");
            put("DBusername", streamDBUsername);
            put("DBpassword", streamDBPassword);
            put("DBstreamdbname", streamdbName);
            put("DBjournaldbname", journaldbName);
            put("queryXML", "<index value=\"example\" operation=\"EQUALS\"/>");
            put("archive.enabled", "true");
        }
    };

    @BeforeEach
    public void setup() {
        // Start mariadb testcontainer with timezone set to America/New_York (UTC-4). Also creates a second streamdb database inside the container alongside the default journaldb.
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername(streamDBUsername).withPassword(streamDBPassword).withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci", "--default-time-zone=" + zoneId.getId()).withDatabaseName(journaldbName).withCopyFileToContainer(MountableFile.forClasspathResource("CREATE_STREAMDB_DB.sql"), "/docker-entrypoint-initdb.d/"));
        mariadb.start();
        connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
        // streamdb and journaldb is populated with test data during MariaDBContainer startup using CREATE_STREAMDB_DB.sql. Logfile table of journaldb is left empty for tests to populate it.
    }

    @AfterEach
    public void cleanup() {
        mariadb.stop();
    }

    private LogfileRecord logfileRecordForEpoch(long epoch, boolean hasNullEpochColumns) {
        Instant instant = Instant.ofEpochSecond(epoch);
        ZonedDateTime zonedDateTime = instant.atZone(zoneId); // expects path dates to be in same timezone as mariadb system timezone
        int year = zonedDateTime.getYear();
        // format 0 in front of string if 1-9
        String month = String.format("%02d", zonedDateTime.getMonthValue());
        String day = String.format("%02d", zonedDateTime.getDayOfMonth());
        String hour = String.format("%02d", zonedDateTime.getHour());

        String filename = "example.log-@" + epoch + "-" + year + month + day + hour + ".log.gz";
        String path = year + "/" + month + "-" + day + "/example.tg.dev.test/example/" + filename;
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(epoch),
                Date.valueOf(zonedDateTime.toLocalDate()),
                Date.valueOf(zonedDateTime.plusYears(1).toLocalDate()),
                UShort.valueOf(1),
                path,
                null,
                UShort.valueOf(1),
                filename,
                new Timestamp(epoch),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                ULong.valueOf(epoch),
                ULong.valueOf(epoch + (365 * 24 * 3600)),
                ULong.valueOf(epoch),
                ULong.valueOf(1)
        );

        LogfileRecord nullEpochRecord = new LogfileRecord(
                ULong.valueOf(epoch),
                Date.valueOf(zonedDateTime.toLocalDate()),
                Date.valueOf(zonedDateTime.plusYears(1).toLocalDate()),
                UShort.valueOf(1),
                path,
                null,
                UShort.valueOf(1),
                filename,
                new Timestamp(epoch),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                null,
                null,
                null,
                ULong.valueOf(1)
        );

        if (hasNullEpochColumns) {
            return nullEpochRecord;
        }
        return logfileRecord;
    }

    /**
     * Testing that pullToSliceTable() pulls only a specific row from database according to the input parameter.
     */
    @Test
    public void pullToSliceTableSingleTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        ZonedDateTime instantPlusDay = instantZonedDateTime.plusDays(1);
        // Set logdate to 2023-10-04 and set logtime-string in path to 2023100422 UTC-4, but set epoch values to null.
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();
        // Set logdate to 2023-10-05 and set logtime-string in path to 2023100522 UTC-4, but set epoch values to null.
        LogfileRecord logfileRecord2 = logfileRecordForEpoch(instantPlusDay.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // Only the row with logdate of "2023-10-4" should be pulled to slicetable.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(1, rows);
            }
        });
    }

    /**
     * Testing that pullToSliceTable() pulls all the rows from database according to the input parameter.
     */
    @Test
    public void pullToSliceTableMultiTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Set logdate to 2023-10-04 and set logtime-string in path to 2023100422 UTC-4, but set epoch values to null.
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        ZonedDateTime instantPlusHour = instantZonedDateTime.plusHours(1);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();
        // Set logdate to 2023-10-04 and set logtime-string in path to 2023100423 UTC-4, but set epoch values to null.
        LogfileRecord logfileRecord2 = logfileRecordForEpoch(instantPlusHour.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // Both of the rows in the database for logdate of "2023-10-4" should be pulled to the slicetable.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(2, rows);
            }
        });
    }

    /**
     * Testing situation where logfile record hasn't been migrated to use epoch columns. Will use old logdate and
     * synthetic logtime fields instead as a fallback which will trigger the session timezone to affect logtime results.
     */
    @Test
    public void epochHourNullTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Set logdate to 2023-10-04 and set logtime-string in path to 2023100422 UTC-4, but set epoch values to null.
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                Instant instantEarliest = Instant.ofEpochSecond(1696392000L);
                ZonedDateTime instantEarliestZonedDateTime = ZonedDateTime.ofInstant(instantEarliest, zoneId);
                long earliestEpoch = instantEarliestZonedDateTime.toEpochSecond(); // 2023-10-04 00:00 UTC-4

                // Pull the records from a specific logdate to the slicetable for further processing.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantEarliestZonedDateTime.toLocalDate()));
                Assertions.assertEquals(1, rows);

                // Get the offset for the first non-empty hour of records from the slicetable.
                WeightedOffset nextHourAndSizeFromSliceTable = sdc.getNextHourAndSizeFromSliceTable(0L);
                Assertions.assertFalse(nextHourAndSizeFromSliceTable.isStub);
                long latestOffset = nextHourAndSizeFromSliceTable.offset();
                // zonedDateTime is used for checking timestamp errors caused by synthetic creation of logtime from logfile path column using regex.
                Assertions.assertEquals(instantZonedDateTime.toEpochSecond(), latestOffset);
                Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> hourRange = sdc
                        .getHourRange(earliestEpoch, latestOffset);
                Assertions.assertEquals(1, hourRange.size());
                // Assert that resulting logfile metadata for logtime is affected by the session timezone when epoch columns are null and session timezone is America/New_York.
                long logtime = hourRange.get(0).get(8, Long.class);
                Assertions.assertEquals(instantZonedDateTime.toEpochSecond(), logtime);
                // Assert that the resulting logfile metadata is as expected for logdate.
                Assertions
                        .assertEquals(Date.valueOf(instantZonedDateTime.toLocalDate()), hourRange.get(0).get(5, Date.class));
            }
        });
    }

    @Test
    public void getNextHourAndSizeFromSliceTableTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Set logdate to 2023-10-04 and set logtime-string in path to 2023100422 UTC-4, but set epoch values to null.
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        ZonedDateTime instantPlusHour = instantZonedDateTime.plusHours(1);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();
        // Set logdate to 2023-10-04 and set logtime-string in path to 2023100423 UTC-4, but set epoch values to null.
        LogfileRecord logfileRecord2 = logfileRecordForEpoch(instantPlusHour.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(2, rows);
                WeightedOffset nextHourAndSizeFromSliceTable = sdc
                        .getNextHourAndSizeFromSliceTable(instantZonedDateTime.toEpochSecond());
                // Assert that the result for next hour from slice table after 2023-10-4 22:00 UTC-4 is 2023-10-4 23:00 UTC-4.
                Assertions.assertEquals(instantPlusHour.toEpochSecond(), nextHourAndSizeFromSliceTable.offset());
            }
        });
    }

    @Test
    public void weightedOffsetTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        final Instant instant = Instant.ofEpochSecond(1696471200L);

        final ZonedDateTime baseTime = ZonedDateTime.ofInstant(instant, zoneId);
        final ZonedDateTime baseMinusOneHour = baseTime.minusHours(1);
        final ZonedDateTime basePlusOneHour = baseTime.plusMinutes(1);
        final ZonedDateTime basePlusOneDay = baseTime.plusDays(1);

        final LogfileRecord baseRecord = logfileRecordForEpoch(baseTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(baseRecord).execute();
        final LogfileRecord plusOneHourRecord = logfileRecordForEpoch(basePlusOneHour.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(plusOneHourRecord).execute();
        final LogfileRecord plusOneDayRecord = logfileRecordForEpoch(basePlusOneDay.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(plusOneDayRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());

        final Config config = new Config(opts);

        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // pull baseTime to SliceTable and assert weightedOffset to contain filesize=240 (2 rows) for that hour
                final int baseTimeRows = sdc.pullToSliceTable(Date.valueOf(baseTime.toLocalDate()));
                Assertions.assertEquals(2, baseTimeRows);
                final WeightedOffset weightedOffsetForBaseTime = sdc
                        .getNextHourAndSizeFromSliceTable(baseMinusOneHour.toEpochSecond());

                Assertions.assertEquals(baseTime.toEpochSecond(), weightedOffsetForBaseTime.offset());
                Assertions.assertEquals(240L, weightedOffsetForBaseTime.fileSize());

                // pull baseTime+1day to SliceTable and assert weightedOffset to contain filesize=120 (1 row) for that hour
                final int plusOneDayRows = sdc.pullToSliceTable(Date.valueOf(basePlusOneDay.toLocalDate()));
                Assertions.assertEquals(1, plusOneDayRows);
                final WeightedOffset weightedOffsetForPlusOneDay = sdc
                        .getNextHourAndSizeFromSliceTable(baseTime.toEpochSecond());
                Assertions.assertEquals(basePlusOneDay.toEpochSecond(), weightedOffsetForPlusOneDay.offset());
                Assertions.assertEquals(120L, weightedOffsetForPlusOneDay.fileSize());
            }
        });
    }

    /**
     * Testing deleteRangeFromSliceTable() method functionality with old logtime implementation.
     */
    @Test
    public void deleteRangeFromSliceTableNullEpochTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Inserting logfile with logtime of 2023-10-04 22:00 UTC-4.
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // Pull the records from a specific logdate to the slicetable for further processing.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(1, rows);
                Assertions.assertFalse(sdc.getNextHourAndSizeFromSliceTable(0L).isStub);

                // Delete rows from slicetable and assert that they are no longer present in the slicetable.
                sdc
                        .deleteRangeFromSliceTable(instantZonedDateTime.minusHours(1).toEpochSecond(), instantZonedDateTime.toEpochSecond());
                Assertions.assertTrue(sdc.getNextHourAndSizeFromSliceTable(0L).isStub);
            }
        });
    }

    /**
     * Testing IncludeBeforeEpoch functionality with old logtime implementation.
     */
    @Test
    public void setIncludeBeforeEpochNullEpochTest() {

        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Inserting logfile with logtime of 2023-10-04 22:00 UTC-4.
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        ZonedDateTime instantPlusHour = instantZonedDateTime.plusHours(1);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();
        // Inserting logfile with logtime of 2023-10-04 23:00 UTC-4.
        LogfileRecord logfileRecord2 = logfileRecordForEpoch(instantPlusHour.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        // Assert StreamDBClient methods work as expected with the test data.

        // Set includeBeforeEpoch in ArchiveConfig to an epoch that represents 2023-10-04 23:00 UTC-4, for getNextHourAndSizeFromSliceTable() to ignore records with logtime of 2023-10-04 23:00 UTC-4 or newer.
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        opts.put("archive.includeBeforeEpoch", String.valueOf(instantPlusHour.toEpochSecond()));
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // Pull the records from a specific logdate to the slicetable for further processing.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(2, rows);

                // find the earliest row and assert that it has correct offset/logtime value
                Assertions.assertFalse(sdc.getNextHourAndSizeFromSliceTable(0L).isStub);
                Assertions
                        .assertEquals(instantZonedDateTime.toEpochSecond(), sdc.getNextHourAndSizeFromSliceTable(0L).offset());
                // find the next row after earliest and assert that it is stub.
                Assertions
                        .assertTrue(sdc.getNextHourAndSizeFromSliceTable(instantZonedDateTime.toEpochSecond()).isStub);
            }
        });
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(StreamDBClient.class)
                .withIgnoredFields("nestedTopNQuery")
                .withPrefabValues(NestedTopNQuery.class, new NestedTopNQuery(null, false), new NestedTopNQuery(null, false)).verify();
    }
}
