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
import com.teragrep.pth_06.jooq.generated.journaldb.tables.records.BucketRecord;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.records.HostRecord;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.records.LogfileRecord;
import com.teragrep.pth_06.jooq.generated.streamdb.tables.records.LogGroupRecord;
import com.teragrep.pth_06.jooq.generated.streamdb.tables.records.StreamRecord;
import org.jooq.*;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.*;
import java.util.HashMap;
import java.util.Map;

import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.pth_06.jooq.generated.streamdb.Streamdb.STREAMDB;

class StreamDBClientTest {

    private final String streamDBUrl = "jdbc:mariadb://localhost:3306/journaldb";
    private final String streamDBUsername = "streamdb";
    private final String streamDBPassword = "streamdb_pass";

    private final String streamdbName = "streamdb";
    private final String journaldbName = "journaldb";
    private final String bloomdbName = "bloomdb";

    @AfterEach
    public void cleanup() {
        // Empty out all the tables from streamdb and journaldb
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(streamdbName), new MappedSchema().withInput("journaldb").withOutput(journaldbName), new MappedSchema().withInput("bloomdb").withOutput(bloomdbName)));
        final Connection connection = Assertions
                .assertDoesNotThrow(() -> DriverManager.getConnection(streamDBUrl, "streamdb", "streamdb_pass"));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        ctx.deleteFrom(JOURNALDB.LOGFILE).execute();
        ctx.deleteFrom(JOURNALDB.HOST).execute();
        ctx.deleteFrom(JOURNALDB.BUCKET).execute();
        ctx.deleteFrom(STREAMDB.STREAM).execute();
        ctx.deleteFrom(STREAMDB.HOST).execute();
        ctx.deleteFrom(STREAMDB.LOG_GROUP).execute();
        ctx.query(String.format("DELETE FROM %s.category", journaldbName)).execute();
        ctx.query(String.format("DELETE FROM %s.source_system", journaldbName)).execute();
    }

    /**
     * Testing situation where epoch_hour is used as a source for logtime and logdate fields.
     */
    @Test
    public void epochHourTest() {
        // Init mandatory Config object.
        Map<String, String> opts = new HashMap<>();
        opts.put("S3endPoint", "mock");
        opts.put("S3identity", "mock");
        opts.put("S3credential", "mock");
        opts.put("DBusername", streamDBUsername);
        opts.put("DBpassword", streamDBPassword);
        opts.put("DBurl", streamDBUrl);
        opts.put("DBstreamdbname", streamdbName);
        opts.put("DBjournaldbname", journaldbName);
        opts.put("num_partitions", "1");
        opts.put("scheduler", "BatchScheduler");
        opts.put("queryXML", "<index value=\"example\" operation=\"EQUALS\"/>");
        // audit information
        opts.put("TeragrepAuditQuery", "index=firewall-data");
        opts.put("TeragrepAuditReason", "test run at fullScanTest()");
        opts.put("TeragrepAuditUser", System.getProperty("user.name"));
        opts.put("archive.enabled", "true");
        // kafka options
        opts.put("kafka.enabled", "false");
        opts
                .put(
                        "kafka.bootstrap.servers",
                        "kafkadev01.example.com:9092,kafkadev02.example.com:9092,kafkadev03.example.com:9092"
                );
        opts.put("kafka.sasl.mechanism", "PLAIN");
        opts.put("kafka.security.protocol", "SASL_PLAINTEXT");
        opts
                .put(
                        "kafka.sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"superuser\" password=\"SuperSecretSuperuserPassword\";"
                );
        opts.put("checkpointLocation", "/tmp/checkpoint");
        final Config config = new Config(opts);

        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(config.archiveConfig.dbStreamDbName), new MappedSchema().withInput("journaldb").withOutput(config.archiveConfig.dbJournalDbName), new MappedSchema().withInput("bloomdb").withOutput(config.archiveConfig.bloomDbName)));
        final Connection connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(
                                        config.archiveConfig.dbUrl, config.archiveConfig.dbUsername,
                                        config.archiveConfig.dbPassword
                                )
                );
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);

        // Add test data to journaldb and streamdb, there are several tables in both databases.

        BucketRecord bucketRecord = new BucketRecord(UShort.valueOf(1), "bucket1");
        ctx.insertInto(JOURNALDB.BUCKET).set(bucketRecord).execute();

        ctx
                .query(String.format("INSERT INTO %s.category (id, name) VALUES (1, 'testCategory')", journaldbName))
                .execute();
        ctx
                .query(
                        String
                                .format(
                                        "INSERT INTO %s.source_system (id, name) VALUES (2, 'testSourceSystem2')",
                                        journaldbName
                                )
                )
                .execute();

        HostRecord hostRecord = new HostRecord(UShort.valueOf(1), "testHost1");
        ctx.insertInto(JOURNALDB.HOST).set(hostRecord).execute();

        // Set logdate to 2023-10-04 instead of the correct 2023-10-05 to emulate timezone issues, and test if epoch_hour takes priority or not.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf(LocalDate.of(2023, 10, 4)),
                new Date(2125 - 1900, 7, 13),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-@1696471200-2023100423.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-@1696471200-2023100505.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "e2I8CnejWweTSk8tmo4tNkDO2fU7RajqI111FPlF7Mw=",
                "5ddea0496b0a9ad1266b26da3de9f573",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                ULong.valueOf(1696471200L),
                ULong.valueOf(4910716800L),
                ULong.valueOf(1755091102L)
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        LogGroupRecord logGroupRecord = new LogGroupRecord(UInteger.valueOf(1), "testGroup1");
        ctx.insertInto(STREAMDB.LOG_GROUP).set(logGroupRecord).execute();

        com.teragrep.pth_06.jooq.generated.streamdb.tables.records.HostRecord host = new com.teragrep.pth_06.jooq.generated.streamdb.tables.records.HostRecord(
                UInteger.valueOf(1),
                "testHost1",
                UInteger.valueOf(1)
        );
        ctx.insertInto(STREAMDB.HOST).set(host).execute();
        StreamRecord streamRecord = new StreamRecord(
                UInteger.valueOf(1),
                UInteger.valueOf(1),
                "example",
                "log:example:0",
                "example"
        );
        ctx.insertInto(STREAMDB.STREAM).set(streamRecord).execute();

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
        // Init mandatory Config object.
        Map<String, String> opts = new HashMap<>();
        opts.put("S3endPoint", "mock");
        opts.put("S3identity", "mock");
        opts.put("S3credential", "mock");
        opts.put("DBusername", streamDBUsername);
        opts.put("DBpassword", streamDBPassword);
        opts.put("DBurl", streamDBUrl);
        opts.put("DBstreamdbname", streamdbName);
        opts.put("DBjournaldbname", journaldbName);
        opts.put("num_partitions", "1");
        opts.put("scheduler", "BatchScheduler");
        opts.put("queryXML", "<index value=\"example\" operation=\"EQUALS\"/>");
        // audit information
        opts.put("TeragrepAuditQuery", "index=firewall-data");
        opts.put("TeragrepAuditReason", "test run at fullScanTest()");
        opts.put("TeragrepAuditUser", System.getProperty("user.name"));
        opts.put("archive.enabled", "true");
        // kafka options
        opts.put("kafka.enabled", "false");
        opts
                .put(
                        "kafka.bootstrap.servers",
                        "kafkadev01.example.com:9092,kafkadev02.example.com:9092,kafkadev03.example.com:9092"
                );
        opts.put("kafka.sasl.mechanism", "PLAIN");
        opts.put("kafka.security.protocol", "SASL_PLAINTEXT");
        opts
                .put(
                        "kafka.sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"superuser\" password=\"SuperSecretSuperuserPassword\";"
                );
        opts.put("checkpointLocation", "/tmp/checkpoint");
        final Config config = new Config(opts);

        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(config.archiveConfig.dbStreamDbName), new MappedSchema().withInput("journaldb").withOutput(config.archiveConfig.dbJournalDbName), new MappedSchema().withInput("bloomdb").withOutput(config.archiveConfig.bloomDbName)));
        final Connection connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(
                                        config.archiveConfig.dbUrl, config.archiveConfig.dbUsername,
                                        config.archiveConfig.dbPassword
                                )
                );
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);

        // Add test data to journaldb and streamdb, there are several tables in both databases.

        BucketRecord bucketRecord = new BucketRecord(UShort.valueOf(1), "bucket1");
        ctx.insertInto(JOURNALDB.BUCKET).set(bucketRecord).execute();

        ctx
                .query(String.format("INSERT INTO %s.category (id, name) VALUES (1, 'testCategory')", journaldbName))
                .execute();
        ctx
                .query(
                        String
                                .format(
                                        "INSERT INTO %s.source_system (id, name) VALUES (2, 'testSourceSystem2')",
                                        journaldbName
                                )
                )
                .execute();

        HostRecord hostRecord = new HostRecord(UShort.valueOf(1), "testHost1");
        ctx.insertInto(JOURNALDB.HOST).set(hostRecord).execute();

        // Set logdate to the correct 2023-10-05 but set epoch values to null.
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(1),
                Date.valueOf(LocalDate.of(2023, 10, 5)),
                new Date(2125 - 1900, 7, 13),
                UShort.valueOf(1),
                "2023/10-05/example.tg.dev.test/example/example.log-2023100505.log.gz",
                null,
                UShort.valueOf(1),
                "example.log-2023100505.log.gz",
                new Timestamp(2025, 8, 13, 16, 18, 22, 0),
                ULong.valueOf(120L),
                "e2I8CnejWweTSk8tmo4tNkDO2fU7RajqI111FPlF7Mw=",
                "5ddea0496b0a9ad1266b26da3de9f573",
                "example",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                null,
                null,
                null
        );
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();

        LogGroupRecord logGroupRecord = new LogGroupRecord(UInteger.valueOf(1), "testGroup1");
        ctx.insertInto(STREAMDB.LOG_GROUP).set(logGroupRecord).execute();

        com.teragrep.pth_06.jooq.generated.streamdb.tables.records.HostRecord host = new com.teragrep.pth_06.jooq.generated.streamdb.tables.records.HostRecord(
                UInteger.valueOf(1),
                "testHost1",
                UInteger.valueOf(1)
        );
        ctx.insertInto(STREAMDB.HOST).set(host).execute();
        StreamRecord streamRecord = new StreamRecord(
                UInteger.valueOf(1),
                UInteger.valueOf(1),
                "example",
                "log:example:0",
                "example"
        );
        ctx.insertInto(STREAMDB.STREAM).set(streamRecord).execute();

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
        // zonedDateTime is used for compensating timestamp errors caused by synthetic creation of logtime from logfile path column using regex.
        ZonedDateTime zonedDateTime = ZonedDateTime.of(2023, 10, 5, 5, 0, 0, 0, ZoneId.systemDefault());
        Assertions.assertEquals(zonedDateTime.toEpochSecond(), latestOffset);
        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> hourRange = sdc
                .getHourRange(earliestEpoch, latestOffset);
        Assertions.assertEquals(1, hourRange.size());
        // Assert that the resulting logfile metadata is as expected for logdate and logtime.
        long logtime = hourRange.get(0).get(8, Long.class);
        Assertions.assertEquals(zonedDateTime.toEpochSecond(), logtime);
        Date logdate = hourRange.get(0).get(5, Date.class);
        Assertions.assertEquals(Date.valueOf(date), logdate);
    }
}
