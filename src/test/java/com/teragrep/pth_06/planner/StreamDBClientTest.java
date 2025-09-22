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
                new Timestamp(1755101902),
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
        // Inserting logfile with logtime of 2023-10-05 03:00.
        LogfileRecord logfileRecord2 = new LogfileRecord(
                ULong.valueOf(2),
                Date.valueOf("2023-10-5"),
                Date.valueOf("2026-10-5"),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-@1696474800-2023100501.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-@1696471200-2023100501.log.gz",
                new Timestamp(1755101902),
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
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        // Assert StreamDBClient methods work as expected with the test data.

        // Set includeBeforeEpoch in ArchiveConfig to an epoch that represents 2023-10-05 02:00, for getNextHourAndSizeFromSliceTable() to ignore records with logtime of 2023-10-05 02:00 or newer.
        ZonedDateTime zonedDateTimeUSA = ZonedDateTime.of(2023, 10, 5, 2, 0, 0, 0, ZoneId.of("America/New_York"));
        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        opts.put("archive.includeBeforeEpoch", String.valueOf(zonedDateTimeUSA.toEpochSecond()));
        final Config config = new Config(opts);
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf("2023-10-5"));
        Assertions.assertEquals(2, rows);

        // Try to get the next hour after 2023-10-05 01:00 from the slicetable, result should be a stub with the includeBeforeEpoch value used.
        Assertions
                .assertTrue(sdc.getNextHourAndSizeFromSliceTable(zonedDateTimeUSA.minusHours(1).toEpochSecond()).isStub);
        // Try to get the next hour after 2023-10-05 00:00 from the slicetable, result should not be a stub.
        Assertions
                .assertFalse(sdc.getNextHourAndSizeFromSliceTable(zonedDateTimeUSA.minusHours(2).toEpochSecond()).isStub);
        // Assert that the resulting WeightedOffset has offset value referencing 2023-10-05 01:00.
        Assertions
                .assertEquals(zonedDateTimeUSA.minusHours(1).toEpochSecond(), sdc.getNextHourAndSizeFromSliceTable(0L).offset());
    }

    /**
     * Testing default IncludeBeforeEpoch functionality with old logtime implementation.
     */
    @Test
    public void setIncludeBeforeEpochDefaultNullEpochTest() {
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
                new Timestamp(1755101902),
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

        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        final Config config = new Config(opts);
        // Assert the default value of includeBeforeEpoch option.
        Assertions.assertEquals(Long.MAX_VALUE, config.archiveConfig.archiveIncludeBeforeEpoch);
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf("2023-10-5"));
        Assertions.assertEquals(1, rows);

        // Try to get the next hour from the slicetable, result should not be a stub with the default includeBeforeEpoch value used.
        Assertions.assertFalse(sdc.getNextHourAndSizeFromSliceTable(0L).isStub);
        ZonedDateTime zonedDateTimeUSA = ZonedDateTime.of(2023, 10, 5, 2, 0, 0, 0, ZoneId.of("America/New_York"));
        Assertions.assertEquals(zonedDateTimeUSA.toEpochSecond(), sdc.getNextHourAndSizeFromSliceTable(0L).offset());
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
