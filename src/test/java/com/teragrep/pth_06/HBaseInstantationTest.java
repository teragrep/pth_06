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

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.LogfileTable;
import com.teragrep.pth_06.planner.MockDBRow;
import com.teragrep.pth_06.planner.MockDBRowSource;
import com.teragrep.pth_06.planner.source.LazySource;
import com.teragrep.pth_06.task.s3.MockS3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLAppStatusStore;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import scala.collection.JavaConverters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class HBaseInstantationTest {

    private SparkSession spark;
    private final String s3endpoint = "http://127.0.0.1:48080";
    private final String s3identity = "s3identity";
    private final String s3credential = "s3credential";
    private String url;
    private final String userName = "sa";
    private final String password = "";
    private final Map<String, String> opts = new HashMap<>();
    private Connection conn;
    private TestingHBaseCluster testCluster;
    private LogfileTable logfileTable;
    private final MockS3Configuration mockS3Configuration = new MockS3Configuration(
            "http://127.0.0.1:48080",
            "s3identity",
            "s3credential"
    );

    private final MockS3 mockS3 = new MockS3(
            mockS3Configuration.s3endpoint(),
            mockS3Configuration.s3identity(),
            mockS3Configuration.s3credential()
    );

    private final MockS3DataProvider mockS3DataProvider = new MockS3DataProvider(
            new MockDBRowSource(),
            mockS3Configuration
    );

    private long totalRows;

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(mockS3::start);
        totalRows = Assertions.assertDoesNotThrow(mockS3DataProvider::preloadS3Data);

        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("queryXML", "query");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", userName);
        opts.put("DBpassword", password);

        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=UCT")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=UCT")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.streaming.metricsEnabled", "true")
                .config("spark.metrics.namespace", "teragrep")
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
        Assertions.assertDoesNotThrow(testCluster::start);
    }

    @AfterAll
    public void stop() {
        if (testCluster.isClusterRunning()) {
            Assertions.assertDoesNotThrow(testCluster::stop);
        }
        Assertions.assertDoesNotThrow(mockS3::stop);
        Assertions.assertDoesNotThrow(logfileTable::close);
    }

    @BeforeEach
    public void beforeEach() {
        url = "jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
        opts.put("DBurl", url);
        conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
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
                .assertDoesNotThrow(() -> new LogfileTable(new Config(opts), new LazySource(testCluster.getConf())), "LogfileTable object should be created");
        final List<MockDBRow> mockDBRows = new ArrayList<>(new MockDBRowSource().asPriorityQueue());
        for (final MockDBRow row : mockDBRows) {
            Assertions
                    .assertDoesNotThrow(
                            () -> logfileTable
                                    .insertRow(
                                            row.id(), row.directory(), row.stream(), row.host(), row.logtag(),
                                            row.logdate(), row.bucket(), row.path(), row.logtime(), row.filesize(),
                                            row.uncompressedFilesize()
                                    )
                    );
        }
        ResultScanner scanner = Assertions
                .assertDoesNotThrow(() -> logfileTable.table().getScanner(new Scan()), "Scanner should be opened to LogfileTable to inspect test data insertion");
        int resultCount = 0;
        for (org.apache.hadoop.hbase.client.Result result : scanner) {
            Assertions.assertFalse(result.isEmpty(), "Scanner should not get an empty result");
            resultCount++;
        }
        Assertions.assertEquals(mockDBRows.size(), resultCount, "Scanner result count should match the test data size");
        scanner.close();
    }

    @AfterEach
    public void close() {
        Assertions.assertDoesNotThrow(conn::close);
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
    public void testHBaseCustomMetrics() {
        final SQLAppStatusStore statusStore = spark.sharedState().statusStore();
        final int oldCount = statusStore.executionsList().size();
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
                .option(
                        "queryXML",
                        "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679201\"/></AND></AND>"
                )
                .option("TeragrepAuditQuery", "index=f17_v2")
                .option("TeragrepAuditReason", "test run at hbaseScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // hbase options
                .option(
                        "hbase.zookeeper.property.clientPort",
                        testCluster.getConf().get("hbase.zookeeper.property.clientPort")
                )
                .option("hbase.master.hostname", testCluster.getConf().get("hbase.master.hostname"))
                .option("hbase.regionserver.hostname", testCluster.getConf().get("hbase.regionserver.hostname"))
                .option("hbase.zookeeper.quorum", testCluster.getConf().get("hbase.zookeeper.quorum"))
                // kafka options
                .option("kafka.enabled", "false")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "false")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();
        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df.writeStream().outputMode(OutputMode.Append()).format("memory").trigger(Trigger.ProcessingTime(0)).queryName("MockArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).option("spark.cleaner.referenceTracking.cleanCheckpoints", "true").start());
        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
        // Metrics
        final Map<String, List<Long>> metricsValues = new HashMap<>();
        while (statusStore.executionsCount() <= oldCount) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        while (statusStore.executionsList().isEmpty() || statusStore.executionsList().last().metricValues() == null) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        statusStore.executionsList(oldCount, (int) (statusStore.executionsCount() - oldCount)).foreach(v1 -> {
            final Map<Object, String> mv = JavaConverters.mapAsJavaMap(v1.metricValues());
            for (final SQLPlanMetric spm : JavaConverters.asJavaIterable(v1.metrics())) {
                final long id = spm.accumulatorId();
                final Object value = mv.get(id);
                if (spm.metricType().startsWith("v2Custom_") && value != null) {
                    final List<Long> preExistingValues = metricsValues
                            .getOrDefault(spm.metricType(), new ArrayList<>());
                    preExistingValues.add(Long.parseLong(value.toString()));
                    metricsValues.put(spm.metricType(), preExistingValues);
                }
            }
            return 0; // need to return something, expects scala Unit return value
        });

        // Check that all expected metrics are present
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.bytes.ArchiveCompressedBytesProcessedMetricAggregator"
                                ),
                        "ArchiveCompressedBytesProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesProcessedMetricAggregator"),
                        "BytesProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesPerSecondMetricAggregator"),
                        "BytesPerSecond metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator"
                                ),
                        "RecordsProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.records.RecordsPerSecondMetricAggregator"
                                ),
                        "RecordsPerSecond metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.objects.ArchiveObjectsProcessedMetricAggregator"
                                ),
                        "ArchiveObjectsProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.records.LatestKafkaTimestampMetricAggregator"
                                ),
                        "LatestKafkaTimestamp metric not present!"
                );

        // default batch size limit object count 5 results in 7 batches from 33 total rows
        Assertions
                .assertEquals(
                        7,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.bytes.ArchiveCompressedBytesProcessedMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        7, metricsValues.get("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesProcessedMetricAggregator").size()
                );
        Assertions
                .assertEquals(
                        7,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.objects.ArchiveObjectsProcessedMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        7,
                        metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator")
                                .size()
                );
        Assertions
                .assertEquals(
                        totalRows,
                        metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator")
                                .stream()
                                .reduce(Long::sum)
                                .orElseGet(Assertions::fail)
                );
        Assertions
                .assertEquals(
                        7,
                        metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.records.RecordsPerSecondMetricAggregator")
                                .size()
                );
        Assertions
                .assertEquals(
                        7, metricsValues.get("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesPerSecondMetricAggregator").size()
                );
    }

    private long resultRowsFromQuery(final String queryString) {
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
                .option("queryXML", queryString)
                // audit information
                .option("TeragrepAuditQuery", "index=f17_v2")
                .option("TeragrepAuditReason", "test run at hbaseScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // hbase options
                .option(
                        "hbase.zookeeper.property.clientPort",
                        testCluster.getConf().get("hbase.zookeeper.property.clientPort")
                )
                .option("hbase.master.hostname", testCluster.getConf().get("hbase.master.hostname"))
                .option("hbase.regionserver.hostname", testCluster.getConf().get("hbase.regionserver.hostname"))
                .option("hbase.zookeeper.quorum", testCluster.getConf().get("hbase.zookeeper.quorum"))
                // kafka options
                .option("kafka.enabled", "false")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "false")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();
        final Dataset<Row> df2 = df.agg(functions.count("*"));
        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df2.writeStream().outputMode("complete").format("memory").trigger(Trigger.ProcessingTime(0)).queryName("HBaseArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).start());
        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        return spark.sqlContext().sql("SELECT * FROM HBaseArchiveQuery").first().getAs(0);
    }

}
