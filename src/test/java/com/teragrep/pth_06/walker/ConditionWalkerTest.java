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
import com.teragrep.pth_06.planner.walker.FilterlessSearchImpl;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Named;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 *
 * @see org.jooq.QueryPart
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConditionWalkerTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    // matches IPv4
    final String ipRegex = "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    // matches IPv4 starting with 255.
    final String ipStartingWith255 = "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    final String uuidPattern = "[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}";
    final List<String> patternList = new ArrayList<>(Arrays.asList(ipRegex, ipStartingWith255, uuidPattern));
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));

    @BeforeAll
    void setup() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS filtertype").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip255").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip192").execute();
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
            String ip192 = "CREATE TABLE `pattern_test_uuid`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(filtertype).execute();
            conn.prepareStatement(ip).execute();
            conn.prepareStatement(ip255).execute();
            conn.prepareStatement(ip192).execute();
            String typeSQL = "INSERT INTO `filtertype` (`id`,`expectedElements`, `targetFpp`, `pattern`) VALUES (?,?,?,?)";
            int id = 1;
            for (String pattern : patternList) {
                PreparedStatement filterType = conn.prepareStatement(typeSQL);
                filterType.setInt(1, id);
                filterType.setInt(2, 1000 * id);
                filterType.setDouble(3, 0.01);
                filterType.setString(4, pattern);
                filterType.executeUpdate();
                id++;
            }
            writeFilter("pattern_test_ip", 1);
            writeFilter("pattern_test_ip255", 2);
            writeFilter("pattern_test_uuid", 3);
        });
    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    void bloomNoMatchTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"nomatch\"/></AND>";
        String e = "\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(0, walker.conditionRequiredTables().size());
    }

    @Test
    void bloomNoMatchStreamQueryTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"nomatch\"/></AND>";
        String e = "\"streamdb\".\"stream\".\"directory\" like 'haproxy'";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, true));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(0, walker.conditionRequiredTables().size());
    }

    @Test
    void bloomNoMatchStreamQueryWithoutFiltersTest() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"nomatch\"/></AND>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, true));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
    }

    @Test
    void singleTablePatternMatchStreamQueryTest() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
        String e = "\"streamdb\".\"stream\".\"directory\" like 'haproxy'";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, true));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(0, walker.conditionRequiredTables().size());
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
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void testWithoutFiltersWithIndex() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<index operation=\"EQUALS\" value=\"haproxy\"/>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void testWithoutFiltersWithSourcetype() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<sourcetype operation=\"EQUALS\" value=\"haproxy\"/>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"getArchivedObjects_filter_table\".\"stream\" like 'haproxy'\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void testWithoutFiltersWithHost() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<host operation=\"EQUALS\" value=\"haproxy\"/>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"getArchivedObjects_filter_table\".\"host\" like 'haproxy'\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void withoutFiltersEnabledWithTimeConstraints() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<AND><index operation=\"EQUALS\" value=\"haproxy\"/><AND><earliest operation=\"GE\" value=\"1643207821\"/><latest operation=\"LE\" value=\"1729435021\"/></AND></AND>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" >= date '2022-01-26'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1643205600)\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" <= date '2024-10-20'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1729435021)\n"
                + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
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
        Assertions.assertEquals(2, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
        Assertions
                .assertTrue(
                        walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip255"))
                );
    }

    @Test
    void testFullXMLTwoMatchingTables() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index operation=\"EQUALS\" value=\"search_bench\"/><AND><AND><AND><earliest operation=\"GE\" value=\"1643207821\"/><latest operation=\"LE\" value=\"1729435021\"/></AND><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND><indexstatement operation=\"EQUALS\" value=\"192.000.1.1\"/></AND></AND>";
        String e = "(\n" + "  \"getArchivedObjects_filter_table\".\"directory\" like 'search_bench'\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" >= date '2022-01-26'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1643205600)\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" <= date '2024-10-20'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1729435021)\n"
                + "  and (\n" + "    (\n" + "      bloommatch(\n" + "        (\n"
                + "          select \"term_0_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n"
                + "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + "  )\n" + "  and (\n" + "    (\n"
                + "      bloommatch(\n" + "        (\n" + "          select \"term_1_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_1_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 1\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n"
                + "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + "  )\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
    }

    @Test
    void testWithoutFiltersEnabled() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<index value=\"haproxy\" operation=\"EQUALS\"/>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
    }

    @Test
    void testWithoutFiltersEnabledWithEarliest() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipRegex));
        String q = "<index value=\"haproxy\" operation=\"EQUALS\"/>";
        String e = "(\n" + "  \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "  and \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" + ")";
        Condition cond = Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        Assertions.assertEquals(e, cond.toString());
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
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
        Assertions.assertEquals(1, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
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
        Assertions.assertEquals(2, walker.conditionRequiredTables().size());
        Assertions
                .assertTrue(walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip")));
        Assertions
                .assertTrue(
                        walker.conditionRequiredTables().stream().anyMatch(t -> t.getName().equals("pattern_test_ip255"))
                );
    }

    @Test
    void testWithoutFiltersOptionActiveWithSearchTermThrowsException() {
        DSLContext ctx = DSL.using(conn);
        ConditionWalker walker = new ConditionWalker(ctx, true, new FilterlessSearchImpl(ctx, ipStartingWith255));
        final String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><AND><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND></AND>";
        final RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> walker.fromString(q));
        final String expectedMessage = "Search terms are not allowed when <bloom.withoutFilters> option is enabled";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void testSinglePatternMatchTempTableValues() {
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));
        ResultSet result = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM term_0_pattern_test_ip").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int colCount = result.getMetaData().getColumnCount();
            Assertions.assertEquals(4, colCount);
            int loops = 0;
            while (result.next()) {
                Assertions.assertEquals(1, result.getLong("id"));
                Assertions.assertEquals(0, result.getLong("term_id"));
                Assertions.assertEquals(1, result.getLong("type_id"));
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(result.getBytes("filter")));
                Assertions.assertTrue(filter.mightContain("192.168.1.1"));
                Assertions.assertFalse(filter.mightContain("192"));
                Assertions.assertFalse(filter.mightContain("192."));
                Assertions.assertFalse(filter.mightContain("192.168.1"));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result.close();
        });
    }

    @Test
    void testMultiplePatternMatchTempTableValues() {
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/></AND>";
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));

        // check pattern_test_ip
        ResultSet result1 = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM term_0_pattern_test_ip").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int colCount = result1.getMetaData().getColumnCount();
            Assertions.assertEquals(4, colCount);
            int loops = 0;
            while (result1.next()) {
                Assertions.assertEquals(1, result1.getLong("id"));
                Assertions.assertEquals(0, result1.getLong("term_id"));
                Assertions.assertEquals(1, result1.getLong("type_id"));
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(result1.getBytes("filter")));
                Assertions.assertEquals(filter.bitSize(), BloomFilter.create(1000, 0.01).bitSize());
                Assertions.assertTrue(filter.mightContain("255.255.255.255"));
                Assertions.assertFalse(filter.mightContain("255"));
                Assertions.assertFalse(filter.mightContain("255."));
                Assertions.assertFalse(filter.mightContain("255.255."));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result1.close();
        });

        // check pattern_test_ip244 table
        ResultSet result2 = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM term_0_pattern_test_ip255").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int colCount = result2.getMetaData().getColumnCount();
            Assertions.assertEquals(4, colCount);
            int loops = 0;
            while (result2.next()) {
                Assertions.assertEquals(1, result2.getLong("id"));
                Assertions.assertEquals(0, result2.getLong("term_id"));
                Assertions.assertEquals(2, result2.getLong("type_id"));
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(result2.getBytes("filter")));
                Assertions.assertEquals(filter.bitSize(), BloomFilter.create(2000, 0.01).bitSize());
                Assertions.assertTrue(filter.mightContain("255.255.255.255"));
                Assertions.assertFalse(filter.mightContain("255"));
                Assertions.assertFalse(filter.mightContain("255."));
                Assertions.assertFalse(filter.mightContain("255.255."));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result2.close();
        });
    }

    @Test
    void testCorrectTokensForTwoSearchTerms() {
        ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        String q = "<AND><index operation=\"EQUALS\" value=\"search_bench\"/><AND><AND><AND><earliest operation=\"GE\" value=\"1643207821\"/><latest operation=\"LE\" value=\"1729435021\"/></AND><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND><indexstatement operation=\"EQUALS\" value=\"192.000.1.1\"/></AND></AND>";
        Assertions.assertDoesNotThrow(() -> walker.fromString(q, false));

        // check term 0
        ResultSet result1 = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM term_0_pattern_test_ip").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int colCount = result1.getMetaData().getColumnCount();
            Assertions.assertEquals(4, colCount);
            int loops = 0;
            while (result1.next()) {
                Assertions.assertEquals(1, result1.getLong("id"));
                Assertions.assertEquals(0, result1.getLong("term_id"));
                Assertions.assertEquals(1, result1.getLong("type_id"));
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(result1.getBytes("filter")));
                Assertions.assertEquals(filter.bitSize(), BloomFilter.create(1000, 0.01).bitSize());
                Assertions.assertTrue(filter.mightContain("192.168.1.1"));
                Assertions.assertFalse(filter.mightContain("192"));
                Assertions.assertFalse(filter.mightContain("168."));
                Assertions.assertFalse(filter.mightContain("1.1"));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result1.close();
        });

        // check term 1
        ResultSet result2 = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM term_1_pattern_test_ip").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int colCount = result2.getMetaData().getColumnCount();
            Assertions.assertEquals(4, colCount);
            int loops = 0;
            while (result2.next()) {
                Assertions.assertEquals(1, result2.getLong("id"));
                Assertions.assertEquals(1, result2.getLong("term_id"));
                Assertions.assertEquals(1, result2.getLong("type_id"));
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(result2.getBytes("filter")));
                Assertions.assertEquals(filter.bitSize(), BloomFilter.create(1000, 0.01).bitSize());
                Assertions.assertTrue(filter.mightContain("192.000.1.1"));
                Assertions.assertFalse(filter.mightContain("192"));
                Assertions.assertFalse(filter.mightContain("000."));
                Assertions.assertFalse(filter.mightContain("192.000"));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result2.close();
        });
    }

    @Test
    public void testTwoIndexStatementsMatchDifferentBloomTablesWithAnd() {
        final ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        final String query = "<AND>" + "<indexstatement operation=\"EQUALS\" value = \"192.168.1.1\"/>"
                + "<indexstatement operation=\"EQUALS\" value = \"123e4567-e89b-12d3-a456-426655440000\"/>" + "</AND>";
        final Condition condition = Assertions.assertDoesNotThrow(() -> walker.fromString(query, false));
        final Set<Table<?>> tables = walker.conditionRequiredTables();
        Assertions.assertEquals(2, tables.size());
        final List<String> tableNames = tables.stream().map(Named::getName).sorted().collect(Collectors.toList());
        final List<String> expectedTableNames = Arrays.asList("pattern_test_ip", "pattern_test_uuid");
        Assertions.assertEquals(expectedTableNames, tableNames);
        final String expected = "(\n" + "  (\n" + "    (\n" + "      bloommatch(\n" + "        (\n"
                + "          select \"term_0_pattern_test_ip\".\"filter\"\n"
                + "          from \"term_0_pattern_test_ip\"\n" + "          where (\n" + "            term_id = 0\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "    )\n"
                + "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + "  )\n" + "  and (\n" + "    (\n"
                + "      bloommatch(\n" + "        (\n" + "          select \"term_1_pattern_test_uuid\".\"filter\"\n"
                + "          from \"term_1_pattern_test_uuid\"\n" + "          where (\n" + "            term_id = 1\n"
                + "            and type_id = \"bloomdb\".\"pattern_test_uuid\".\"filter_type_id\"\n" + "          )\n"
                + "        ),\n" + "        \"bloomdb\".\"pattern_test_uuid\".\"filter\"\n" + "      ) = true\n"
                + "      and \"bloomdb\".\"pattern_test_uuid\".\"filter\" is not null\n" + "    )\n"
                + "    or \"bloomdb\".\"pattern_test_uuid\".\"filter\" is null\n" + "  )\n" + ")";
        Assertions.assertEquals(expected, condition.toString());
    }

    @Test
    public void testTwoIndexStatementsMatchDifferentBloomTablesWithOr() {
        final ConditionWalker walker = new ConditionWalker(DSL.using(conn), true);
        final String query = "<OR>" + "<indexstatement operation=\"EQUALS\" value = \"192.168.1.1\"/>"
                + "<indexstatement operation=\"EQUALS\" value = \"123e4567-e89b-12d3-a456-426655440000\"/>" + "</OR>";
        final Condition condition = Assertions.assertDoesNotThrow(() -> walker.fromString(query, false));
        final Set<Table<?>> tables = walker.conditionRequiredTables();
        Assertions.assertEquals(2, tables.size());
        final List<String> tableNames = tables.stream().map(Named::getName).sorted().collect(Collectors.toList());
        final List<String> expectedTableNames = Arrays.asList("pattern_test_ip", "pattern_test_uuid");
        Assertions.assertEquals(expectedTableNames, tableNames);
        final String expected = "(\n" + "  (\n" + "    bloommatch(\n" + "      (\n"
                + "        select \"term_0_pattern_test_ip\".\"filter\"\n" + "        from \"term_0_pattern_test_ip\"\n"
                + "        where (\n" + "          term_id = 0\n"
                + "          and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "        )\n"
                + "      ),\n" + "      \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "    ) = true\n"
                + "    and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "  )\n"
                + "  or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + "  or (\n" + "    bloommatch(\n"
                + "      (\n" + "        select \"term_1_pattern_test_uuid\".\"filter\"\n"
                + "        from \"term_1_pattern_test_uuid\"\n" + "        where (\n" + "          term_id = 1\n"
                + "          and type_id = \"bloomdb\".\"pattern_test_uuid\".\"filter_type_id\"\n" + "        )\n"
                + "      ),\n" + "      \"bloomdb\".\"pattern_test_uuid\".\"filter\"\n" + "    ) = true\n"
                + "    and \"bloomdb\".\"pattern_test_uuid\".\"filter\" is not null\n" + "  )\n"
                + "  or \"bloomdb\".\"pattern_test_uuid\".\"filter\" is null\n" + ")";

        Assertions.assertEquals(expected, condition.toString());
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
            stmt.close();
        });
    }
}
