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

import com.teragrep.pth_06.ast.analyze.ScanPlan;
import com.teragrep.pth_06.ast.analyze.ScanPlanImpl;
import com.teragrep.pth_06.ast.analyze.ScanPlanView;
import com.teragrep.pth_06.ast.analyze.View;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.source.LazySource;
import com.teragrep.pth_06.task.s3.MockS3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SynchronizedHourlyResultsTest {

    private final String s3endpoint = "http://127.0.0.1:48080";
    private final String s3identity = "s3identity";
    private final String s3credential = "s3credential";
    private final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    private final String userName = "sa";
    private final String password = "";
    private final Connection conn = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
    private final Map<String, String> opts = new HashMap<>();
    private TestingHBaseCluster testCluster;
    private LogfileTable logfileTable;
    private final MockS3 mockS3 = new MockS3(s3endpoint, s3identity, s3credential);

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(mockS3::start);

        opts
                .put(
                        "queryXML",
                        "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679200\"/></AND></AND>"
                );
        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", userName);
        opts.put("DBpassword", password);
        opts.put("DBurl", url);
        opts.put("quantumLength", "15");

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
        Assertions.assertDoesNotThrow(logfileTable::close);
        Assertions.assertDoesNotThrow(mockS3::stop);
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
        });

        Assertions.assertTrue(testCluster.isClusterRunning());
        logfileTable = Assertions
                .assertDoesNotThrow(() -> new LogfileTable(new Config(opts), new LazySource(testCluster.getConf())));
        TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> virtualDatabaseMap = new MockDBData()
                .getVirtualDatabaseMap();
        Assertions.assertDoesNotThrow(() -> logfileTable.insertResults(virtualDatabaseMap.values()));
        ResultScanner scanner = Assertions.assertDoesNotThrow(() -> logfileTable.table().getScanner(new Scan()));
        int resultCount = 0;
        for (org.apache.hadoop.hbase.client.Result result : scanner) {
            Assertions.assertFalse(result.isEmpty());
            resultCount++;
        }
        Assertions.assertEquals(virtualDatabaseMap.size(), resultCount);
        scanner.close();
    }

    @Test
    public void testSingleView() {
        ScanPlan scanPlan = new ScanPlanImpl(1, 1, 1362296800, new FilterList());
        ScanPlanView scanPlanView = new ScanPlanView(scanPlan, logfileTable);
        List<View> views = Collections.singletonList(scanPlanView);
        SynchronizedHourlyResults synchronizedHourlyResults = new SynchronizedHourlyResults(views, 1262296800);
        List<org.apache.hadoop.hbase.client.Result> results = synchronizedHourlyResults.nextHour();

        Assertions.assertEquals(1, results.size());
        ByteBuffer wrap = ByteBuffer.wrap(results.get(0).getRow());
        long streamId = wrap.getLong();
        long earliest = wrap.getLong();
        long id = wrap.getLong();
        Assertions.assertEquals(1, streamId);
        Assertions.assertEquals(1262296800L, earliest);
        Assertions.assertEquals(28653, id);
    }

    @Test
    public void testMultipleHourlyResults() {
        ScanPlan scanPlan = new ScanPlanImpl(1, 1, 1362296800, new FilterList());
        ScanPlanView scanPlanView = new ScanPlanView(scanPlan, logfileTable);
        List<View> views = Collections.singletonList(scanPlanView);
        SynchronizedHourlyResults synchronizedHourlyResults = new SynchronizedHourlyResults(views, 1262296800);
        List<org.apache.hadoop.hbase.client.Result> firstHourResults = synchronizedHourlyResults.nextHour();
        Assertions.assertEquals(1, firstHourResults.size());
        List<org.apache.hadoop.hbase.client.Result> secondHourResults = synchronizedHourlyResults.nextHour();
        Assertions.assertEquals(1, secondHourResults.size());
        ByteBuffer wrap = ByteBuffer.wrap(secondHourResults.get(0).getRow());
        long streamId = wrap.getLong();
        long earliest = wrap.getLong();
        long id = wrap.getLong();
        Assertions.assertEquals(1, streamId);
        Assertions.assertEquals(1262300400L, earliest);
        Assertions.assertEquals(28703, id);
    }

    @Test
    public void multipleScanRangeViewTest() {
        ScanPlan scanPlan1 = new ScanPlanImpl(1, 1, 1362296800, new FilterList());
        ScanPlan scanPlan2 = new ScanPlanImpl(1, 1, 1362296800, new FilterList());
        ScanPlanView scanPlanView1 = new ScanPlanView(scanPlan1, logfileTable);
        ScanPlanView scanPlanView2 = new ScanPlanView(scanPlan2, logfileTable);
        List<View> views = Arrays.asList(scanPlanView1, scanPlanView2);
        SynchronizedHourlyResults synchronizedHourlyResults = new SynchronizedHourlyResults(views, 1262296800);
        List<org.apache.hadoop.hbase.client.Result> results = synchronizedHourlyResults.nextHour();

        Assertions.assertEquals(2, results.size());
        ByteBuffer wrap = ByteBuffer.wrap(results.get(0).getRow());
        ByteBuffer wrap2 = ByteBuffer.wrap(results.get(1).getRow());
        Assertions.assertArrayEquals(wrap.array(), wrap2.array());
        long streamId = wrap.getLong();
        long earliest = wrap.getLong();
        long id = wrap.getLong();
        Assertions.assertEquals(1, streamId);
        Assertions.assertEquals(1262296800L, earliest);
        Assertions.assertEquals(28653, id);
    }

    @Test
    public void testHasNextReturnsFalseAfterAllViewsFinished() {
        ScanPlan scanPlan = new ScanPlanImpl(1, 1, 1262296800, new FilterList());
        ScanPlanView scanPlanView = new ScanPlanView(scanPlan, logfileTable);
        SynchronizedHourlyResults syncResults = new SynchronizedHourlyResults(
                Collections.singletonList(scanPlanView),
                1262296800
        );
        while (syncResults.hasNext()) {
            syncResults.nextHour();
        }
        Assertions.assertFalse(syncResults.hasNext());
    }
}
