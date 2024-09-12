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

import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.DSLContext;
import org.jooq.Named;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PatternMatchTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    final List<String> patternList = new ArrayList<>(
            Arrays
                    .asList(
                            "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}",
                            "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}"
                    )
    );
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));

    @BeforeAll
    void setup() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS filtertype").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip255").execute();
            String filtertype = "CREATE TABLE`filtertype`" + "("
                    + "    `id`               bigint(20) unsigned   NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `expectedElements` bigint(20) unsigned NOT NULL,"
                    + "    `targetFpp`        DOUBLE(2) unsigned NOT NULL,"
                    + "    `pattern`          VARCHAR(2048) NOT NULL,"
                    + "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`)" + ")";
            String ip = "CREATE TABLE `pattern_test_ip`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            String ip255 = "CREATE TABLE `pattern_test_ip255`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(filtertype).execute();
            conn.prepareStatement(ip).execute();
            conn.prepareStatement(ip255).execute();
            String typeSQL = "INSERT INTO `filtertype` (`id`,`expectedElements`, `targetFpp`, `pattern`) VALUES (?,?,?,?)";
            int id = 1;
            for (String pattern : patternList) {
                PreparedStatement filterType = conn.prepareStatement(typeSQL);
                filterType.setInt(1, id);
                filterType.setInt(2, 1000);
                filterType.setDouble(3, 0.01);
                filterType.setString(4, pattern);
                filterType.executeUpdate();
                id++;
            }
            writeFilter("pattern_test_ip", 1);
            writeFilter("pattern_test_ip255", 2);
        });
    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP ALL OBJECTS").execute(); //h2 clear database
            conn.close();
        });
    }

    @Test
    public void testSingleMatch() {
        DSLContext ctx = DSL.using(conn);
        String input = "192.168.1.1";
        PatternMatch patternMatch = new PatternMatch(ctx, input);
        List<Table<?>> result = patternMatch.toList();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("pattern_test_ip", result.get(0).getName());
    }

    @Test
    public void testSearchTermTokenizedMatch() {
        DSLContext ctx = DSL.using(conn);
        String input = "target_ip=192.168.1.1";
        PatternMatch patternMatch = new PatternMatch(ctx, input);
        List<Table<?>> result = patternMatch.toList();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("pattern_test_ip", result.get(0).getName());
    }

    @Test
    public void testMultipleMatch() {
        DSLContext ctx = DSL.using(conn);
        String input = "255.255.255.255";
        PatternMatch patternMatch = new PatternMatch(ctx, input);
        List<Table<?>> result = patternMatch.toList();
        List<Table<?>> result2 = patternMatch.toList();
        List<String> tableNames = result.stream().map(Named::getName).collect(Collectors.toList());
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(2, result2.size());
        Assertions.assertTrue(tableNames.contains("pattern_test_ip"));
        Assertions.assertTrue(tableNames.contains("pattern_test_ip255"));
    }

    @Test
    public void testNoMatch() {
        DSLContext ctx = DSL.using(conn);
        String input = "testinput";
        PatternMatch patternMatch = new PatternMatch(ctx, input);
        List<Table<?>> result = patternMatch.toList();
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void equalsTest() {
        DSLContext ctx = DSL.using(conn);
        String input = "testinput";
        PatternMatch eq1 = new PatternMatch(ctx, input);
        PatternMatch eq2 = new PatternMatch(ctx, input);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq2, eq1);
    }

    @Test
    public void differentInputNotEqualsTest() {
        DSLContext ctx = DSL.using(conn);
        PatternMatch eq1 = new PatternMatch(ctx, "testinput");
        PatternMatch eq2 = new PatternMatch(ctx, "anotherinput");
        Assertions.assertNotEquals(eq1, eq2);
        Assertions.assertNotEquals(eq2, eq1);
    }

    @Test
    public void differentDSLContextNotEqualsTest() {
        DSLContext ctx1 = DSL.using(conn);
        DSLContext ctx2 = DSL.using(conn);
        PatternMatch eq1 = new PatternMatch(ctx1, "testinput");
        PatternMatch eq2 = new PatternMatch(ctx2, "testinput");
        Assertions.assertNotEquals(eq1, eq2);
        Assertions.assertNotEquals(eq2, eq1);
    }

    private void writeFilter(String tableName, int filterId) {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            String sql = "INSERT INTO `" + tableName + "` (`partition_id`, `filter_type_id`, `filter`) "
                    + "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            BloomFilter filter = BloomFilter.create(1000, 0.01);
            final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                filter.writeTo(filterBAOS);
                filterBAOS.close();
            });
            stmt.setInt(1, 1);
            stmt.setInt(2, filterId);
            stmt.setBytes(3, filterBAOS.toByteArray());
            stmt.executeUpdate();
        });
    }
}
