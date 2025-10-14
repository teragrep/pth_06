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
package com.teragrep.pth_06;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.SDElement;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.LogfileTable;
import com.teragrep.pth_06.planner.MockDBData;
import com.teragrep.pth_06.task.s3.MockS3;
import com.teragrep.pth_06.task.s3.Pth06S3Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HBaseInstantationTest {

    private SparkSession spark;
    private final String s3endpoint = "http://127.0.0.1:48080";
    private final String s3identity = "s3identity";
    private final String s3credential = "s3credential";
    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
    final Map<String, String> opts = new HashMap<>();
    TestingHBaseCluster testCluster;
    LogfileTable logfileTable;
    private final MockS3 mockS3 = new MockS3(s3endpoint, s3identity, s3credential);
    long totalRows;

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(mockS3::start);
        totalRows = Assertions.assertDoesNotThrow(this::preloadS3Data);

        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("queryXML", "query");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", userName);
        opts.put("DBpassword", password);
        opts.put("DBurl", url);

        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=UCT")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=UCT")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        final TestingHBaseClusterOption clusterOption = TestingHBaseClusterOption
                .builder()
                .numMasters(1)
                .numRegionServers(1)
                .build();
        testCluster = TestingHBaseCluster.create(clusterOption);
        Configuration conf = testCluster.getConf();
        conf.set("hbase.master.hostname", "localhost");
        conf.set("hbase.regionserver.hostname", "localhost");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Assertions.assertDoesNotThrow(testCluster::start);
    }

    @AfterAll
    public void stop() {
        if (testCluster.isClusterRunning()) {
            Assertions.assertDoesNotThrow(testCluster::stop);
        }
        Assertions.assertDoesNotThrow(conn::close);
        Assertions.assertDoesNotThrow(mockS3::stop);
        Assertions.assertDoesNotThrow(logfileTable::close);
    }

    @BeforeEach
    public void beforeEach() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS STREAMDB").execute();
            conn.prepareStatement("USE STREAMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS host").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS stream").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS log_group").execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `log_group` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  PRIMARY KEY (`id`)\n" + ")"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `host` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `gid` int(10) unsigned NOT NULL,\n" + "  PRIMARY KEY (`id`),\n"
                                    + "  KEY `host_gid` (`gid`),\n" + "  KEY `idx_name_id` (`name`,`id`),\n"
                                    + "  CONSTRAINT `host_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE\n"
                                    + ")"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `stream` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `gid` int(10) unsigned NOT NULL,\n"
                                    + "  `directory` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `stream` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `tag` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  PRIMARY KEY (`id`),\n" + "  KEY `stream_gid` (`gid`),\n"
                                    + "  CONSTRAINT `stream_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE\n"
                                    + ") "
                    )
                    .execute();
            // add expected values contained in mock data
            conn.prepareStatement("INSERT INTO `log_group` (`name`) VALUES ('test_group');").execute();
            conn.prepareStatement("INSERT INTO `host` (`name`, `gid`) VALUES ('sc-99-99-14-108', 1);").execute();
            conn
                    .prepareStatement(
                            "INSERT INTO `stream` (`gid`, `directory`, `stream`, `tag`) VALUES (1, 'f17_v2', 'log:f17_v2:0', 'test_tag');"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "INSERT INTO `stream` (`gid`, `directory`, `stream`, `tag`) VALUES (1, 'f17', 'log:f17', 'test_tag');"
                    )
                    .execute();
        }, "SQL test database initialization and population should not fail");

        Assertions.assertTrue(testCluster.isClusterRunning(), "Hbase test cluster should be running");
        logfileTable = Assertions
                .assertDoesNotThrow(() -> new LogfileTable(testCluster.getConf(), new Config(opts)), "LogfileTable object should be created");
        TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> virtualDatabaseMap = new MockDBData()
                .getVirtualDatabaseMap();
        Assertions
                .assertDoesNotThrow(() -> logfileTable.insertResults(virtualDatabaseMap.values()), "Test data should be correctly inserted to LogfileTable");
        ResultScanner scanner = Assertions
                .assertDoesNotThrow(() -> logfileTable.table().getScanner(new Scan()), "Scanner should be opened to LogfileTable to inspect test data insertion");
        int resultCount = 0;
        for (org.apache.hadoop.hbase.client.Result result : scanner) {
            Assertions.assertFalse(result.isEmpty(), "Scanner should not get an empty result");
            resultCount++;
        }
        Assertions
                .assertEquals(
                        virtualDatabaseMap.size(), resultCount, "Scanner result count should match the test data size"
                );
        scanner.close();
    }

    @Test
    public void scanAllRowsTest() {
        // earliest epoch in test data 1262296800
        // latest epoch in test data 1263679200
        // query latest is exclusive so end epoch of 1263679201 used to get all rows
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679201\"/></AND></AND>";
        final long rows = resultRowsFromQuery(query);
        Assertions.assertEquals(totalRows, rows);
    }

    @Test
    public void NoResultRowsTest() {
        // range outside of test rows
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1\"/><latest operation=\"EQUALS\" value=\"100000\"/></AND></AND>";
        final long rows = resultRowsFromQuery(query);
        Assertions.assertEquals(0, rows);
    }

    @Test
    public void excludeStartingRowsTest() {
        // query start after first row
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296801\"/><latest operation=\"EQUALS\" value=\"1263679201\"/></AND></AND>";
        final long rows = resultRowsFromQuery(query);
        Assertions.assertEquals(totalRows - 1, rows);
    }

    @Test
    public void excludeEndRowsTest() {
        // query stop on last row, end is exclusive so not in result
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679200\"/></AND></AND>";
        final long rows = resultRowsFromQuery(query);
        Assertions.assertEquals(totalRows - 1, rows);
    }

    @Test
    public void testBatchInHours() {

        final List<StreamingQueryProgress> progresses = new ArrayList<>();

        spark.streams().addListener(new StreamingQueryListener() {

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                progresses.add(event.progress());
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {

            }

            @Override
            public void onQueryStarted(QueryStartedEvent event) {

            }
        });
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679201\"/></AND></AND>";

        final Dataset<Row> df = spark
                .readStream()
                .format(TeragrepDatasource.class.getName())
                .option("archive.enabled", "true")
                .option("hbase.enabled", "true")
                .option("S3endPoint", s3endpoint)
                .option("S3identity", s3identity)
                .option("S3credential", s3credential)
                .option("DBusername", userName)
                .option("DBpassword", password)
                .option("DBurl", url)
                .option("DBstreamdbname", "streamdb")
                .option("DBjournaldbname", "journaldb")
                .option("num_partitions", "1")
                .option("queryXML", query)
                // audit information
                .option("TeragrepAuditQuery", "index=f17_v2")
                .option("TeragrepAuditReason", "test run at hbaseScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "false")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "false")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();

        Dataset<Row> df2 = df.agg(functions.count("*"));

        DataStreamWriter<Row> dfWriter = df2
                .writeStream()
                .outputMode("complete")
                .format("memory")
                .trigger(Trigger.AvailableNow())
                .queryName("HourlyTestQuery")
                .option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID())
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true");

        StreamingQuery streamingQuery = Assertions.assertDoesNotThrow(() -> dfWriter.start());
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
        System.out.println("PROGRESSES: " + progresses.size());
    }

    private long resultRowsFromQuery(final String queryString) {
        Dataset<Row> df = spark
                .readStream()
                .format(TeragrepDatasource.class.getName())
                .option("archive.enabled", "true")
                .option("hbase.enabled", "true")
                .option("S3endPoint", s3endpoint)
                .option("S3identity", s3identity)
                .option("S3credential", s3credential)
                .option("DBusername", userName)
                .option("DBpassword", password)
                .option("DBurl", url)
                .option("DBstreamdbname", "streamdb")
                .option("DBjournaldbname", "journaldb")
                .option("num_partitions", "1")
                .option("queryXML", queryString)
                // audit information
                .option("TeragrepAuditQuery", "index=f17_v2")
                .option("TeragrepAuditReason", "test run at hbaseScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "false")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "false")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();

        Dataset<Row> df2 = df.agg(functions.count("*"));

        DataStreamWriter<Row> dfWriter = df2
                .writeStream()
                .outputMode("complete")
                .format("memory")
                .trigger(Trigger.ProcessingTime(0))
                .queryName("HBaseArchiveQuery")
                .option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID())
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true");

        StreamingQuery streamingQuery = Assertions.assertDoesNotThrow(() -> dfWriter.start());

        DataStreamWriter<Row> rowDataStreamWriter = df.writeStream().foreachBatch((ds, id) -> {
            ds.show(false);
        });
        StreamingQuery sq = Assertions.assertDoesNotThrow(() -> rowDataStreamWriter.start());

        sq.processAllAvailable();
        Assertions.assertDoesNotThrow(sq::stop);
        Assertions.assertDoesNotThrow(() -> sq.awaitTermination());

        long rowCount = 0;
        while (Assertions.assertDoesNotThrow(() -> !streamingQuery.awaitTermination(1000))) {

            long resultSize = spark.sqlContext().sql("SELECT * FROM HBaseArchiveQuery").count();
            if (resultSize > 0) {
                rowCount = spark.sqlContext().sql("SELECT * FROM HBaseArchiveQuery").first().getAs(0);
            }
            if (
                streamingQuery.lastProgress() == null
                        || streamingQuery.status().message().equals("Initializing sources")
            ) {
                // queryString has not started
            }
            else if (streamingQuery.lastProgress().sources().length != 0) {
                if (isArchiveDone(streamingQuery)) {
                    Assertions.assertDoesNotThrow(streamingQuery::stop);
                }
            }
        }
        return rowCount;
    }

    private boolean isArchiveDone(StreamingQuery outQ) {
        boolean archiveDone = true;
        for (int i = 0; i < outQ.lastProgress().sources().length; i++) {
            String startOffset = outQ.lastProgress().sources()[i].startOffset();
            String endOffset = outQ.lastProgress().sources()[i].endOffset();
            String description = outQ.lastProgress().sources()[i].description();

            if (description != null && !description.startsWith("com.teragrep.pth_06.ArchiveMicroStreamReader@")) {
                // ignore others than archive
                continue;
            }

            if (startOffset != null) {
                if (!startOffset.equalsIgnoreCase(endOffset)) {
                    archiveDone = false;
                }
            }
            else {
                archiveDone = false;
            }
        }
        return archiveDone;
    }

    private long preloadS3Data() throws IOException {
        long rows = 0L;
        AmazonS3 amazonS3 = new Pth06S3Client(s3endpoint, s3identity, s3credential).build();

        TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> virtualDatabaseMap = new MockDBData()
                .getVirtualDatabaseMap();

        for (
            Map.Entry<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> entry : virtualDatabaseMap
                    .entrySet()
        ) {
            for (
                Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record10 : entry
                        .getValue()
            ) {
                String host = record10.get(3, String.class);
                String logtag = record10.get(4, String.class);
                String bucket = record10.get(6, String.class);
                String path = record10.get(7, String.class);
                long logtime = record10.get(8, ULong.class).longValue();

                String recordAsJson = record10.formatJSON();

                // <46>1 2010-01-01T12:34:56.123456+02:00 hostname.domain.tld pstats - -
                SyslogMessage syslog = new SyslogMessage();
                syslog = syslog
                        .withFacility(Facility.USER)
                        .withSeverity(Severity.WARNING)
                        .withTimestamp(logtime)
                        .withHostname(host)
                        .withAppName(logtag)
                        .withMsg(recordAsJson);

                // [event_id@48577 hostname="hostname.domain.tld" uuid="" unixtime="" id_source="source"]

                SDElement event_id_48577 = new SDElement("event_id@48577")
                        .addSDParam("hostname", host)
                        .addSDParam("uuid", UUID.randomUUID().toString())
                        .addSDParam("source", "source")
                        .addSDParam("unixtime", Long.toString(System.currentTimeMillis()));

                syslog = syslog.withSDElement(event_id_48577);

                // [event_format@48577 original_format="rfc5424"]

                SDElement event_format_48577 = new SDElement("event_id@48577").addSDParam("original_format", "rfc5424");

                syslog = syslog.withSDElement(event_format_48577);

                // [event_node_relay@48577 hostname="relay.domain.tld" source="hostname.domain.tld" source_module="imudp"]

                SDElement event_node_relay_48577 = new SDElement("event_node_relay@48577")
                        .addSDParam("hostname", "relay.domain.tld")
                        .addSDParam("source", host)
                        .addSDParam("source_module", "imudp");

                syslog = syslog.withSDElement(event_node_relay_48577);

                // [event_version@48577 major="2" minor="2" hostname="relay.domain.tld" version_source="relay"]

                SDElement event_version_48577 = new SDElement("event_version@48577")
                        .addSDParam("major", "2")
                        .addSDParam("minor", "2")
                        .addSDParam("hostname", "relay.domain.tld")
                        .addSDParam("version_source", "relay");

                syslog = syslog.withSDElement(event_version_48577);

                // [event_node_router@48577 source="relay.domain.tld" source_module="imrelp" hostname="router.domain.tld"]

                SDElement event_node_router_48577 = new SDElement("event_node_router@48577")
                        .addSDParam("source", "relay.domain.tld")
                        .addSDParam("source_module", "imrelp")
                        .addSDParam("hostname", "router.domain.tld");

                syslog = syslog.withSDElement(event_node_router_48577);

                // [origin@48577 hostname="original.hostname.domain.tld"]

                SDElement origin_48577 = new SDElement("origin@48577")
                        .addSDParam("hostname", "original.hostname.domain.tld");
                syslog = syslog.withSDElement(origin_48577);

                // check if this bucket exists
                boolean bucketExists = false;
                for (Bucket existingBucket : amazonS3.listBuckets()) {
                    if (existingBucket.getName().equals(bucket)) {
                        bucketExists = true;
                        break;
                    }
                }
                if (!bucketExists) {
                    amazonS3.createBucket(bucket);
                }

                // compress the message
                String syslogMessage = syslog.toRfc5424SyslogMessage();

                ByteArrayOutputStream outStream = new ByteArrayOutputStream(syslogMessage.length());
                GZIPOutputStream gzip = new GZIPOutputStream(outStream);
                gzip.write(syslogMessage.getBytes());
                gzip.close();

                ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

                // upload as file
                amazonS3.putObject(bucket, path, inStream, null);
                rows++;
            }

        }
        return rows;
    }
}
