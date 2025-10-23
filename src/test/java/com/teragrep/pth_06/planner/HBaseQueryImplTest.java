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
import com.teragrep.pth_06.planner.source.LazySource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.spark.sql.SparkSession;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HBaseQueryImplTest {

    private SparkSession spark;
    private final String s3endpoint = "http://127.0.0.1:48080";
    private final String s3identity = "s3identity";
    private final String s3credential = "s3credential";
    String url;
    final String userName = "sa";
    final String password = "";
    final Map<String, String> opts = new HashMap<>();
    Connection conn;
    TestingHBaseCluster testCluster;
    LogfileTable logfileTable;

    @BeforeAll
    public void setup() {
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679201\"/></AND></AND>";
        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("kafka.enabled", "false");
        opts.put("queryXML", query);
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

    @AfterEach
    public void close() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testOpen() {
        Config config = new Config(opts);
        try (HBaseQuery hBaseQuery = new HBaseQueryImpl(config, new LazySource(testCluster.getConf()))) {
            Assertions.assertFalse(hBaseQuery.hasNext());
            Assertions.assertDoesNotThrow(() -> hBaseQuery.open(0));
            Assertions.assertTrue(hBaseQuery.hasNext());
        }
    }

    @Test
    public void testEarliest() {
        final Config config = new Config(opts);
        try (final HBaseQuery hBaseQuery = new HBaseQueryImpl(config, new LazySource(testCluster.getConf()))) {
            Assertions.assertEquals(1262296800, hBaseQuery.earliest());
        }
    }

    @Test
    public void testLatest() {
        final Config config = new Config(opts);
        try (final HBaseQuery hBaseQuery = new HBaseQueryImpl(config, new LazySource(testCluster.getConf()))) {
            Assertions.assertEquals(1262469600, hBaseQuery.latest());
        }
    }

    @Test
    public void testMostRecentOffsetAtStart() {
        final Config config = new Config(opts);
        try (final HBaseQuery hBaseQuery = new HBaseQueryImpl(config, new LazySource(testCluster.getConf()))) {
            Assertions.assertEquals(1262296800, hBaseQuery.mostRecentOffset());
        }
    }

    @Test
    public void testMostRecentAfterBatch() {
        final Config config = new Config(opts);
        try (final HBaseQuery hBaseQuery = new HBaseQueryImpl(config, new LazySource(testCluster.getConf()))) {
            final long earliest = hBaseQuery.earliest();
            hBaseQuery.latest(); // increment first batch
            hBaseQuery.open(earliest);
            final int size = hBaseQuery.currentBatch().size();
            Assertions.assertEquals(5, size);
            Assertions.assertEquals(1262296800, hBaseQuery.mostRecentOffset());
            Assertions.assertTrue(hBaseQuery.hasNext());
        }
    }
}
