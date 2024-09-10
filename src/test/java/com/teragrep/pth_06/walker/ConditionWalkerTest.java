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
package com.teragrep.pth_06.walker;

import com.teragrep.pth_06.planner.walker.ConditionWalker;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConditionWalkerTest {

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
    void bloomNoMatchTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"nomatch\"/></AND>";
        String e = "\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(0, walker.patternMatchTables().size());
    }

    @Test
    void bloomNoMatchStreamQueryTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"nomatch\"/></AND>";
        String e = "\"streamdb\".\"stream\".\"directory\" like 'haproxy'";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, true));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(0, walker.patternMatchTables().size());
    }

    @Test
    void singleTablePatternMatchStreamQueryTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
        String e = "\"streamdb\".\"stream\".\"directory\" like 'haproxy'";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, true));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(0, walker.patternMatchTables().size());
    }

    @Test
    void singleTablePatternMatchTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
        String e = "(\n" + "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" + "  and (\n"
                + "    (\n" + "      bloommatch(\n" + "        (\n"
                + "          select \"term_0_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n"
                + "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + "  )\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.patternMatchTables().size());
        Assertions
                .assertTrue(walker.patternMatchTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void twoTablePatternMatchTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/></AND>";
        String e = "(\n" + "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" + "  and (\n"
                + "    (\n" + "      bloommatch(\n" + "        (\n"
                + "          select \"term_0_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n" + "    or (\n"
                + "      bloommatch(\n" + "        (\n" + "          select \"term_0_pattern_test_ip255\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip255\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip255\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip255\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is not null\n" + "    )\n" + "    or (\n"
                + "      \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is null\n" + "    )\n" + "  )\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(2, walker.patternMatchTables().size());
        Assertions
                .assertTrue(walker.patternMatchTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
        Assertions
                .assertTrue(walker.patternMatchTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip255")));
    }

    @Test
    void multipleSearchTermTestOneMatchTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><indexstatement operation=\"EQUALS\" value=\"nomatch\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
        String e = "(\n" + "  (\n" + "    bloommatch(\n" + "      (\n"
                + "        select \"term_1_pattern_test_ip\".\"filter\"\n" + "        from \"term_1_pattern_test_ip\"\n"
                + "        where (\n" + "          term_id = 1\n"
                + "          and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "        )\n"
                + "      ),\n" + "      \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "    ) = true\n"
                + "    and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "  )\n"
                + "  or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.patternMatchTables().size());
        Assertions
                .assertTrue(walker.patternMatchTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void multipleSearchTermTwoAndOneMatchTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
        String e = "(\n" + "  (\n" + "    (\n" + "      bloommatch(\n" + "        (\n"
                + "          select \"term_0_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n" + "    or (\n"
                + "      bloommatch(\n" + "        (\n" + "          select \"term_0_pattern_test_ip255\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip255\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip255\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip255\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is not null\n" + "    )\n" + "    or (\n"
                + "      \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is null\n" + "    )\n" + "  )\n"
                + "  and (\n" + "    (\n" + "      bloommatch(\n" + "        (\n"
                + "          select \"term_1_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_1_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 1\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n"
                + "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + "  )\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(2, walker.patternMatchTables().size());
        Assertions
                .assertTrue(walker.patternMatchTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
        Assertions
                .assertTrue(walker.patternMatchTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip255")));
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
