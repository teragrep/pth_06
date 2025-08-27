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
import org.jooq.*;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
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
        // Start mariadb testcontainer with timezone set to America/New_York. Also creates a second streamdb database inside the container alongside the default journaldb.
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername(streamDBUsername).withPassword(streamDBPassword).withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci", "--default-time-zone=America/New_York").withDatabaseName(journaldbName).withCopyFileToContainer(MountableFile.forClasspathResource("CREATE_STREAMDB_DB.sql"), "/docker-entrypoint-initdb.d/"));
        mariadb.start();
        connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );

        // Create table JOURNALDB.SOURCE_SYSTEM
        Assertions
                .assertDoesNotThrow(
                        () -> connection
                                .prepareStatement(
                                        "CREATE TABLE `source_system` (\n"
                                                + "  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,\n"
                                                + "  `name` varchar(175) NOT NULL COMMENT 'Source system''s name',\n"
                                                + "  PRIMARY KEY (`id`),\n"
                                                + "  UNIQUE KEY `uix_source_system_name` (`name`)\n"
                                                + ") ENGINE=InnoDB AUTO_INCREMENT=113 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for different applications.'"
                                )
                                .execute()
                );
        // Create table JOURNALDB.CATEGORY
        Assertions
                .assertDoesNotThrow(
                        () -> connection
                                .prepareStatement(
                                        "CREATE TABLE `category` (\n"
                                                + "  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,\n"
                                                + "  `name` varchar(175) DEFAULT NULL COMMENT 'Category''s name',\n"
                                                + "  PRIMARY KEY (`id`),\n"
                                                + "  UNIQUE KEY `uix_category_name` (`name`)\n"
                                                + ") ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for different categories.';"
                                )
                                .execute()
                );
        // Create table JOURNALDB.BUCKET
        Assertions
                .assertDoesNotThrow(
                        () -> connection
                                .prepareStatement(
                                        "CREATE TABLE `bucket` (\n"
                                                + "  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,\n"
                                                + "  `name` varchar(64) NOT NULL COMMENT 'Name of the bucket',\n"
                                                + "  PRIMARY KEY (`id`),\n"
                                                + "  UNIQUE KEY `uix_bucket_name` (`name`)\n"
                                                + ") ENGINE=InnoDB AUTO_INCREMENT=92 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Buckets in object storage';"
                                )
                                .execute()
                );
        // Create table JOURNALDB.HOST
        Assertions
                .assertDoesNotThrow(
                        () -> connection
                                .prepareStatement(
                                        "CREATE TABLE `host` (\n"
                                                + "  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,\n"
                                                + "  `name` varchar(175) NOT NULL COMMENT 'Name of the host',\n"
                                                + "  PRIMARY KEY (`id`),\n" + "  UNIQUE KEY `uix_host_name` (`name`)\n"
                                                + ") ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Host names';"
                                )
                                .execute()
                );
        // Create table JOURNALDB.LOGFILE
        Assertions
                .assertDoesNotThrow(
                        () -> connection
                                .prepareStatement(
                                        "CREATE TABLE `logfile` (\n"
                                                + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"
                                                + "  `logdate` date NOT NULL COMMENT 'Log file''s date',\n"
                                                + "  `expiration` date NOT NULL COMMENT 'Log file''s expiration date',\n"
                                                + "  `bucket_id` smallint(5) unsigned NOT NULL COMMENT 'Reference to bucket table',\n"
                                                + "  `path` varchar(2048) NOT NULL COMMENT 'Log file''s path in object storage',\n"
                                                + "  `object_key_hash` char(64) GENERATED ALWAYS AS (sha2(concat(`path`,`bucket_id`),256)) STORED COMMENT 'Hash of path and bucket_id for uniqueness checks. Known length: 64 characters (SHA-256)',\n"
                                                + "  `host_id` smallint(5) unsigned NOT NULL COMMENT 'Reference to host table',\n"
                                                + "  `original_filename` varchar(255) NOT NULL COMMENT 'Log file''s original file name',\n"
                                                + "  `archived` datetime NOT NULL COMMENT 'Date and time when the log file was archived',\n"
                                                + "  `file_size` bigint(20) unsigned NOT NULL DEFAULT 0 COMMENT 'Log file''s size in bytes',\n"
                                                + "  `sha256_checksum` char(44) NOT NULL COMMENT 'An SHA256 hash of the log file (Note: known to be 44 characters long)',\n"
                                                + "  `archive_etag` varchar(64) NOT NULL COMMENT 'Object storage''s MD5 hash of the log file (Note: room left for possible implementation changes)',\n"
                                                + "  `logtag` varchar(48) NOT NULL COMMENT 'A link back to CFEngine',\n"
                                                + "  `source_system_id` smallint(5) unsigned NOT NULL COMMENT 'Log file''s source system (references source_system.id)',\n"
                                                + "  `category_id` smallint(5) unsigned NOT NULL DEFAULT 0 COMMENT 'Log file''s category (references category.id)',\n"
                                                + "  `uncompressed_file_size` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  uncompressed file size',\n"
                                                + "  `epoch_hour` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  epoch logdate',\n"
                                                + "  `epoch_expires` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  epoch expiration',\n"
                                                + "  `epoch_archived` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  epoch archived',\n"
                                                + "  PRIMARY KEY (`id`),\n"
                                                + "  UNIQUE KEY `uix_logfile_object_hash` (`object_key_hash`),\n"
                                                + "  KEY `bucket_id` (`bucket_id`),\n"
                                                + "  KEY `category_id` (`category_id`),\n"
                                                + "  KEY `ix_logfile_expiration` (`expiration`),\n"
                                                + "  KEY `ix_logfile__source_system_id` (`source_system_id`),\n"
                                                + "  KEY `cix_logfile_logdate_host_id_logtag` (`logdate`,`host_id`,`logtag`),\n"
                                                + "  KEY `cix_logfile_host_id_logtag_logdate` (`host_id`,`logtag`,`logdate`),\n"
                                                + "  KEY `cix_logfile_epoch_hour_host_id_logtag` (`epoch_hour`,`host_id`,`logtag`),\n"
                                                + "  KEY `ix_logfile_epoch_expires` (`epoch_expires`),\n"
                                                + "  CONSTRAINT `fk_logfile__source_system_id` FOREIGN KEY (`source_system_id`) REFERENCES `source_system` (`id`),\n"
                                                + "  CONSTRAINT `logfile_ibfk_1` FOREIGN KEY (`bucket_id`) REFERENCES `bucket` (`id`),\n"
                                                + "  CONSTRAINT `logfile_ibfk_2` FOREIGN KEY (`host_id`) REFERENCES `host` (`id`),\n"
                                                + "  CONSTRAINT `logfile_ibfk_4` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)\n"
                                                + ") ENGINE=InnoDB AUTO_INCREMENT=135 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for log files that have been run through Log Archiver';"
                                )
                                .execute()
                );

        // Insert test data for all the tables inside journaldb except for logfile.
        Assertions
                .assertDoesNotThrow(
                        () -> connection.prepareStatement("INSERT INTO host (id, name) VALUES (1, 'testHost1')").execute()
                );
        Assertions
                .assertDoesNotThrow(
                        () -> connection.prepareStatement("INSERT INTO bucket (id, name) VALUES (1, 'bucket1')").execute()
                );
        Assertions
                .assertDoesNotThrow(
                        () -> connection.prepareStatement("INSERT INTO category (id, name) VALUES (1, 'testCategory')").execute()
                );
        Assertions
                .assertDoesNotThrow(
                        () -> connection
                                .prepareStatement("INSERT INTO source_system (id, name) VALUES (2, 'testSourceSystem2')").execute()
                );
        // streamdb is created and populated by test data during MariaDBContainer startup.
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
     * Testing situation where epoch_hour is used as a source for logtime field and epoch_archived for logdate field.
     */
    @Test
    public void epochHourTest() {
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
        Config config = new Config(opts);
        // Add test data to logfile table in journaldb.
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(streamdbName), new MappedSchema().withInput("journaldb").withOutput(journaldbName), new MappedSchema().withInput("bloomdb").withOutput(bloomdbName)));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);

        // Set logdate to 2023-10-04 instead of the correct 2023-10-05 to emulate timezone issues, and test if epoch_hour takes priority or not.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf(LocalDate.of(2023, 10, 4)),
                Date.valueOf(LocalDate.of(2028, 8, 13)),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-@1696471200-2023100423.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-@1696471200-2023100423.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                ULong.valueOf(1696471200L),
                ULong.valueOf(4910716800L),
                ULong.valueOf(1696464000L)
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));
        sdc.setIncludeBeforeEpoch(Long.MAX_VALUE);
        Long earliestEpoch = 1696377600L; // 2023-10-04
        LocalDate date = LocalDate.of(2023, 10, 5);
        Long latestOffset = earliestEpoch;

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf(date));
        Assertions.assertEquals(1, rows);

        // Get the offset for the first non-empty hour of records from the slicetable.
        WeightedOffset nextHourAndSizeFromSliceTable = sdc.getNextHourAndSizeFromSliceTable(latestOffset);
        Assertions.assertFalse(nextHourAndSizeFromSliceTable.isStub);
        latestOffset = nextHourAndSizeFromSliceTable.offset();
        Assertions.assertEquals(1696471200L, latestOffset);
        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> hourRange = sdc
                .getHourRange(earliestEpoch, latestOffset);
        Assertions.assertEquals(1, hourRange.size());
        // Assert that the resulting logfile metadata is as expected for logdate and logtime.
        long logtime = hourRange.get(0).get(8, Long.class);
        Assertions.assertEquals(1696471200L, logtime);
        Date logdate = hourRange.get(0).get(5, Date.class);
        Assertions.assertEquals(Date.valueOf(date), logdate);
    }

    /**
     * Testing situation where logfile record hasn't been migrated to use epoch columns. Will use old logdate and
     * synthetic logtime fields instead as a fallback.
     */
    @Test
    public void epochHourNullTest() {
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
        Config config = new Config(opts);
        // Add test data to logfile table in journaldb.
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(config.archiveConfig.dbStreamDbName), new MappedSchema().withInput("journaldb").withOutput(config.archiveConfig.dbJournalDbName), new MappedSchema().withInput("bloomdb").withOutput(config.archiveConfig.bloomDbName)));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        // Set logdate to the correct 2023-10-05 but set epoch values to null.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf(LocalDate.of(2023, 10, 5)),
                Date.valueOf(LocalDate.of(2028, 8, 13)),
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
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));
        sdc.setIncludeBeforeEpoch(Long.MAX_VALUE);
        Long earliestEpoch = 1696377600L; // 2023-10-04
        LocalDate date = LocalDate.of(2023, 10, 5);
        Long latestOffset = earliestEpoch;

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf(date));
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
        // Assert that resulting logfile metadata for logtime is affected by the session timezone when epoch columns are null.
        long logtime = hourRange.get(0).get(8, Long.class);
        Assertions.assertNotEquals(zonedDateTimeUTC.toEpochSecond(), logtime);
        Assertions.assertEquals(zonedDateTimeUSA.toEpochSecond(), logtime);
        // Assert that the resulting logfile metadata is as expected for logdate.
        Date logdate = hourRange.get(0).get(5, Date.class);
        Assertions.assertEquals(Date.valueOf(date), logdate);
    }

    /**
     * Testing timezone handling of epoch_hour and logtime near midnight, check ci.yaml for possible errors as ci tests
     * use GMT+0 instead of GMT+3.
     */
    @Test
    public void epochHourTimezoneTest() {
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
        Config config = new Config(opts);
        // Add test data to logfile table in journaldb.
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(config.archiveConfig.dbStreamDbName), new MappedSchema().withInput("journaldb").withOutput(config.archiveConfig.dbJournalDbName), new MappedSchema().withInput("bloomdb").withOutput(config.archiveConfig.bloomDbName)));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);

        // Set epoch_hour to 2023-10-05 23:00 UTC, which will cause issues if timezones are affecting logtime and logdate.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf(LocalDate.of(2023, 10, 5)),
                Date.valueOf(LocalDate.of(2028, 8, 13)),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-@1696546800-2023100523.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-@1696546800-2023100523.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                ULong.valueOf(1696546800L),
                ULong.valueOf(4910716800L),
                ULong.valueOf(1696464000L)
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        // Assert StreamDBClient methods work as expected with the test data.
        final StreamDBClient sdc = Assertions.assertDoesNotThrow(() -> new StreamDBClient(config));
        sdc.setIncludeBeforeEpoch(Long.MAX_VALUE);
        Long earliestEpoch = 1696377600L; // 2023-10-04
        LocalDate date = LocalDate.of(2023, 10, 5);
        Long latestOffset = earliestEpoch;

        // Pull the records from a specific logdate to the slicetable for further processing.
        int rows = sdc.pullToSliceTable(Date.valueOf(date));
        Assertions.assertEquals(1, rows);

        // Get the offset for the first non-empty hour of records from the slicetable.
        WeightedOffset nextHourAndSizeFromSliceTable = sdc.getNextHourAndSizeFromSliceTable(latestOffset);
        Assertions.assertFalse(nextHourAndSizeFromSliceTable.isStub);
        latestOffset = nextHourAndSizeFromSliceTable.offset();
        Assertions.assertEquals(1696546800L, latestOffset);
        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> hourRange = sdc
                .getHourRange(earliestEpoch, latestOffset);
        Assertions.assertEquals(1, hourRange.size());
        // Assert that the resulting logfile metadata is as expected for logdate and logtime.
        long logtime = hourRange.get(0).get(8, Long.class);
        Assertions.assertEquals(1696546800L, logtime);
        Date logdate = hourRange.get(0).get(5, Date.class);
        Assertions.assertEquals(Date.valueOf(date), logdate);
    }
}
