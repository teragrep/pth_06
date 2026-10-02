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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogfileTableTest {

    private final Map<String, String> opts = new HashMap<>();
    private TestingHBaseCluster testCluster;
    private final TableName defaultName = TableName.valueOf("logfile");
    private final TableName customName = TableName.valueOf("customName");

    @BeforeAll
    public void setup() {

        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("queryXML", "query");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", "sa");
        opts.put("DBpassword", "");
        opts.put("DBurl", "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE");

        final TestingHBaseClusterOption clusterOption = TestingHBaseClusterOption
                .builder()
                .numMasters(1)
                .numRegionServers(1)
                .build();
        testCluster = TestingHBaseCluster.create(clusterOption);
        final Configuration conf = testCluster.getConf();
        conf.set("hbase.master.hostname", "localhost");
        conf.set("hbase.regionserver.hostname", "localhost");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Assertions.assertDoesNotThrow(testCluster::start);
    }

    @BeforeEach
    public void deleteTables() {
        Assertions.assertDoesNotThrow(() -> {
            try (final Connection connection = ConnectionFactory.createConnection(testCluster.getConf())) {
                try (final Admin admin = connection.getAdmin()) {
                    if (admin.tableExists(defaultName)) {
                        admin.disableTable(defaultName);
                        admin.deleteTable(defaultName);
                    }
                    if (admin.tableExists(customName)) {
                        admin.disableTable(customName);
                        admin.deleteTable(customName);
                    }
                }
            }
        });
    }

    @AfterAll
    public void stop() {
        if (testCluster.isClusterRunning()) {
            Assertions.assertDoesNotThrow(testCluster::stop);
        }
    }

    @Test
    public void testDefaultLogfileTableName() {
        LogfileTable logfileTable = new LogfileTable(new Config(opts), testCluster.getConf());
        Table table = logfileTable.table();
        Assertions.assertEquals(defaultName, table.getName());
        Assertions.assertDoesNotThrow(logfileTable::close);
    }

    @Test
    public void testLogfileTableNameOption() {
        HashMap<String, String> optsWithLogfileTableName = new HashMap<>(opts);
        optsWithLogfileTableName.put("hbase.tablename", "customName");
        LogfileTable logfileTable = new LogfileTable(new Config(optsWithLogfileTableName), testCluster.getConf());
        Assertions.assertDoesNotThrow(() -> {
            try (Table table = logfileTable.table()) {
                Assertions.assertEquals(customName, table.getName());
            }
        });
        Assertions.assertDoesNotThrow(logfileTable::close);
    }

    @Test
    public void testMultipleTableCalls() {
        Configuration conf = testCluster.getConf();
        LogfileTable logfileTable = new LogfileTable(new Config(opts), conf);
        Connection connection = Assertions.assertDoesNotThrow(() -> ConnectionFactory.createConnection(conf));
        Assertions.assertDoesNotThrow(() -> {
            try (Admin admin = connection.getAdmin()) {
                Assertions.assertFalse(admin.tableExists(defaultName));
                try (Table table = logfileTable.table()) {
                    Assertions.assertEquals(defaultName, table.getName());
                }
                Assertions.assertTrue(admin.tableExists(defaultName));
                try (Table table = logfileTable.table()) {
                    Assertions.assertEquals(defaultName, table.getName());
                }
            }
        });
        Assertions.assertDoesNotThrow(connection::close);
    }
}
