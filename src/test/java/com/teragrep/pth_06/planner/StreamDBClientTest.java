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

    private final String streamDBUsername = "streamdb";
    private final String streamDBPassword = "streamdb_pass";

    private final String streamdbName = "streamdb";
    private final String journaldbName = "journaldb";
    private final String bloomdbName = "bloomdb";

    @BeforeEach
    public void setup() {
        // Start mariadb testcontainer with timezone set to America/New_York (UTC-4). Also creates a second streamdb database inside the container alongside the default journaldb.
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername(streamDBUsername).withPassword(streamDBPassword).withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci", "--default-time-zone=America/New_York").withDatabaseName(journaldbName).withCopyFileToContainer(MountableFile.forClasspathResource("CREATE_STREAMDB_DB.sql"), "/docker-entrypoint-initdb.d/"));
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
        // Drop tables from journaldb
        Assertions
                .assertDoesNotThrow(
                        () -> connection.prepareStatement("DROP TABLE logfile, host, bucket, category, source_system").execute()
                );
        // Drop tables from streamdb
        Connection streamdbConnection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(
                                        "jdbc:mariadb://" + mariadb.getHost() + ":" + mariadb.getFirstMappedPort() + "/"
                                                + streamdbName,
                                        mariadb.getUsername(), mariadb.getPassword()
                                )
                );
        Assertions
                .assertDoesNotThrow(() -> streamdbConnection.prepareStatement("DROP TABLE stream, host, log_group").execute());
        mariadb.stop();
    }

    private Config testConfiguration() {
        // Init mandatory Config object with the minimum options required for testing StreamDBClient.
        Map<String, String> opts = new HashMap<>();
        opts.put("S3endPoint", "mock");
        opts.put("S3identity", "mock");
        opts.put("S3credential", "mock");
        opts.put("DBusername", streamDBUsername);
        opts.put("DBpassword", streamDBPassword);
        opts.put("DBurl", mariadb.getJdbcUrl());
        opts.put("DBstreamdbname", streamdbName);
        opts.put("DBjournaldbname", journaldbName);
        opts.put("queryXML", "<index value=\"example\" operation=\"EQUALS\"/>");
        opts.put("archive.enabled", "true");
        return new Config(opts);
    }

    private Config testConfiguration(long includeBeforeEpoch) {
        // Init mandatory Config object with the minimum options required for testing StreamDBClient.
        Map<String, String> opts = new HashMap<>();
        opts.put("S3endPoint", "mock");
        opts.put("S3identity", "mock");
        opts.put("S3credential", "mock");
        opts.put("DBusername", streamDBUsername);
        opts.put("DBpassword", streamDBPassword);
        opts.put("DBurl", mariadb.getJdbcUrl());
        opts.put("DBstreamdbname", streamdbName);
        opts.put("DBjournaldbname", journaldbName);
        opts.put("queryXML", "<index value=\"example\" operation=\"EQUALS\"/>");
        opts.put("archive.enabled", "true");
        opts.put("archive.includeBeforeEpoch", String.valueOf(includeBeforeEpoch));
        return new Config(opts);
    }

    /**
     * Testing situation where logfile record hasn't been migrated to use epoch columns. Will use old logdate and
     * synthetic logtime fields instead as a fallback which will trigger the session timezone to affect logtime results.
     */
    @Test
    public void epochHourNullTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Set logdate to 2023-10-05 and set logtime-string in path to 2023100505, but set epoch values to null.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf("2023-10-5"),
                Date.valueOf("2026-10-5"),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-2023100505.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-2023100505.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                null,
                null,
                null
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Config config = testConfiguration();
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));
        Long earliestEpoch = 1696377600L; // 2023-10-04
        Long latestOffset = earliestEpoch;

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf("2023-10-5"));
        Assertions.assertEquals(1, rows);

        // Get the offset for the first non-empty hour of records from the slicetable.
        WeightedOffset nextHourAndSizeFromSliceTable = sdc.getNextHourAndSizeFromSliceTable(latestOffset);
        Assertions.assertFalse(nextHourAndSizeFromSliceTable.isStub);
        latestOffset = nextHourAndSizeFromSliceTable.offset();
        // zonedDateTime is used for checking timestamp errors caused by synthetic creation of logtime from logfile path column using regex.
        ZonedDateTime zonedDateTimeUTC = ZonedDateTime.of(2023, 10, 5, 5, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime zonedDateTimeUSA = ZonedDateTime.of(2023, 10, 5, 5, 0, 0, 0, ZoneId.of("America/New_York"));
        Assertions.assertNotEquals(zonedDateTimeUTC.toEpochSecond(), latestOffset);
        Assertions.assertEquals(zonedDateTimeUSA.toEpochSecond(), latestOffset);
        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> hourRange = sdc
                .getHourRange(earliestEpoch, latestOffset);
        Assertions.assertEquals(1, hourRange.size());
        // Assert that resulting logfile metadata for logtime is affected by the session timezone when epoch columns are null and session timezone is America/New_York.
        long logtime = hourRange.get(0).get(8, Long.class);
        Assertions.assertNotEquals(zonedDateTimeUTC.toEpochSecond(), logtime);
        Assertions.assertEquals(zonedDateTimeUSA.toEpochSecond(), logtime);
        // Assert that the resulting logfile metadata is as expected for logdate.
        Assertions.assertEquals(Date.valueOf("2023-10-5"), hourRange.get(0).get(5, Date.class));
    }

    /**
     * Testing deleteRangeFromSliceTable() method functionality with old logtime implementation.
     */
    @Test
    public void deleteRangeFromSliceTableNullEpochTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Inserting logfile with logtime of 2023-10-05 02:00 UTC.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf("2023-10-5"),
                Date.valueOf("2026-10-5"),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-@1696471200-2023100502.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-@1696471200-2023100502.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                null,
                null,
                null
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final Config config = testConfiguration();
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf("2023-10-5"));
        Assertions.assertEquals(1, rows);
        Assertions.assertFalse(sdc.getNextHourAndSizeFromSliceTable(1696377600L).isStub);

        // Delete rows from slicetable and assert that they are no longer present in the slicetable.
        ZonedDateTime zonedDateTimeUSA = ZonedDateTime.of(2023, 10, 5, 2, 0, 0, 0, ZoneId.of("America/New_York"));
        sdc.deleteRangeFromSliceTable(1696377600L, zonedDateTimeUSA.toEpochSecond());
        Assertions.assertTrue(sdc.getNextHourAndSizeFromSliceTable(1696377600L).isStub);
    }

    /**
     * Testing IncludeBeforeEpoch functionality with old logtime implementation.
     */
    @Test
    public void setIncludeBeforeEpochNullEpochTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        // Inserting logfile with logtime of 2023-10-05 02:00.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf("2023-10-5"),
                Date.valueOf("2026-10-5"),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-@1696471200-2023100502.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-@1696471200-2023100502.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                null,
                null,
                null
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.

        // Set includeBeforeEpoch in ArchiveConfig to an epoch that represents 2023-10-05 02:00, for getNextHourAndSizeFromSliceTable() to ignore records with logtime of 2023-10-05 02:00 or newer.
        ZonedDateTime zonedDateTimeUSA = ZonedDateTime.of(2023, 10, 5, 2, 0, 0, 0, ZoneId.of("America/New_York"));
        final Config config = testConfiguration(zonedDateTimeUSA.toEpochSecond());
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf("2023-10-5"));
        Assertions.assertEquals(1, rows);

        // Try to get the next hour from the slicetable, result should be a stub.
        Assertions.assertTrue(sdc.getNextHourAndSizeFromSliceTable(zonedDateTimeUSA.toEpochSecond() - 3600L).isStub);
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(StreamDBClient.class)
                .withNonnullFields(
                        "metricRegistry", "ctx", "includeBeforeEpoch", "bloomEnabled", "journaldbCondition", "walker"
                )
                .withIgnoredFields("LOGGER")
                .verify();
    }
}
