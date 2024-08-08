/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
import org.jooq.DSLContext;
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
public class XmlWalkerTest {
    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    ConditionWalker conditionWalker;
    ConditionWalker streamWalker;
    Connection conn;


    @BeforeAll
    public void setup() {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
        Assertions.assertDoesNotThrow(() -> {
            List<String> patternList = new ArrayList<>(Arrays.asList(
                    "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}",
                    "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}"
            ));
            Connection conn = DriverManager.getConnection(url, userName, password);
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS bloomdb").execute();
            conn.prepareStatement("USE bloomdb").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS filtertype").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS pattern_test_ip255").execute();
            String filtertype = "CREATE TABLE`filtertype`" +
                    "(" +
                    "    `id`               bigint(20) unsigned   NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `expectedElements` bigint(20) unsigned NOT NULL," +
                    "    `targetFpp`        DOUBLE(2) unsigned NOT NULL," +
                    "    `pattern`          VARCHAR(2048) NOT NULL," +
                    "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`)" +
                    ")";
            String ip = "CREATE TABLE `pattern_test_ip`(" +
                    "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE," +
                    "    `filter_type_id` bigint(20) unsigned NOT NULL," +
                    "    `filter`         longblob            NOT NULL)";
            String ip255 = "CREATE TABLE `pattern_test_ip255`(" +
                    "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE," +
                    "    `filter_type_id` bigint(20) unsigned NOT NULL," +
                    "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(filtertype).execute();
            conn.prepareStatement(ip).execute();
            conn.prepareStatement(ip255).execute();
            String typeSQL = "INSERT INTO `filtertype` (`id`,`expectedElements`, `targetFpp`, `pattern`) VALUES (?,?,?,?)";
            int id = 1;
            for (String pattern : patternList) {
                PreparedStatement filterType = conn.prepareStatement(typeSQL);
                filterType.setInt(1, id);
                filterType.setInt(2, 100);
                filterType.setDouble(3, 0.01);
                filterType.setString(4, pattern);
                filterType.executeUpdate();
                id++;
            }
            writeFilter("pattern_test_ip", 1, conn);
            writeFilter("pattern_test_ip255", 2, conn);
            this.conn = conn;
        });
    }

    @BeforeEach
    public void beforeEach() {
        DSLContext ctx = DSL.using(conn);
        this.conditionWalker = new ConditionWalker(ctx, false, true);
        this.streamWalker = new ConditionWalker(ctx, true);
    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP ALL OBJECTS").execute(); //h2 clear database
            conn.close();
        });
    }

    @Test
    void fromStringTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<index value=\"haproxy\" operation=\"NOT_EQUALS\"/>";
            String e = "not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringAndTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND>";
            String e = "(\n" +
                    "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n" +
                    "  and \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    ")";
            Condition cond = streamWalker.fromString(q);
            String result = cond.toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringOrNETest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<OR><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
            String e = "(\n" +
                    "  not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n" +
                    "  or \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    ")";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }


    @Test
    void fromStringNotTest() {
        Assertions.assertDoesNotThrow(() -> {
            // index=cpu sourcetype=log:cpu:0 NOT src
            String q = "<OR><index value=\"cpu\" operation=\"EQUALS\"/><sourcetype value=\"log:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
            String e = "(\n" +
                    "  \"streamdb\".\"stream\".\"directory\" like 'cpu'\n" +
                    "  or \"streamdb\".\"stream\".\"stream\" like 'log:haproxy:haproxy'\n" +
                    ")";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringNot1Test() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<AND><AND><index operation=\"EQUALS\" value=\"cpu\"/><sourcetype operation=\"EQUALS\" value=\"log:cpu:0\"/></AND><NOT><indexstatement operation=\"EQUALS\" value=\"src\"/></NOT></AND>";
            String e = "(\n" +
                    "  \"streamdb\".\"stream\".\"directory\" like 'cpu'\n" +
                    "  and \"streamdb\".\"stream\".\"stream\" like 'log:cpu:0'\n" +
                    ")";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringOrAndTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
            String e = "(\n" +
                    "  \"streamdb\".\"stream\".\"directory\" like '%'\n" +
                    "  or (\n" +
                    "    \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n" +
                    "    and \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    "  )\n" +
                    ")";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringOrAnd1Test() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
            String e = "(\n" +
                    "  \"streamdb\".\"stream\".\"directory\" like '%'\n" +
                    "  or (\n" +
                    "    \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n" +
                    "    and \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    "  )\n" +
                    ")";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringDropTimesTest() {
        Assertions.assertDoesNotThrow(() -> {
            // Drop index string and earliest from query
            String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
            String e = "(\n" +
                    "  (\n" +
                    "    not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n" +
                    "    and \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    "    and \"streamdb\".\"host\".\"name\" like 'zong.xnet.fi'\n" +
                    "  )\n" +
                    "  or (\n" +
                    "    \"streamdb\".\"stream\".\"directory\" like '%'\n" +
                    "    and \"streamdb\".\"host\".\"name\" like 'salengar.xnet.fi'\n" +
                    "  )\n" +
                    ")";
            String result = streamWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringTimeRangesTest() {
        Assertions.assertDoesNotThrow(() -> {
            // Drop index string and earliest from query
            String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
            String e = "(\n" +
                    "  (\n" +
                    "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                    "    and \"getArchivedObjects_filter_table\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    "    and \"getArchivedObjects_filter_table\".\"host\" like 'zong.xnet.fi'\n" +
                    "  )\n" +
                    "  or (\n" +
                    "    \"getArchivedObjects_filter_table\".\"directory\" like '%'\n" +
                    "    and \"getArchivedObjects_filter_table\".\"host\" like 'salengar.xnet.fi'\n" +
                    "    and \"journaldb\".\"logfile\".\"logdate\" >= date '2021-01-26'\n" +
                    "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1611655200)\n" +
                    "  )\n" +
                    ")";
            String result = conditionWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringTimeRangesUsingEpochTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
            String e = "(\n" +
                    "  (\n" +
                    "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                    "    and \"getArchivedObjects_filter_table\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                    "    and \"getArchivedObjects_filter_table\".\"host\" like 'zong.xnet.fi'\n" +
                    "  )\n" +
                    "  or (\n" +
                    "    \"getArchivedObjects_filter_table\".\"directory\" like '%'\n" +
                    "    and \"getArchivedObjects_filter_table\".\"host\" like 'salengar.xnet.fi'\n" +
                    "    and \"journaldb\".\"logfile\".\"logdate\" >= date '2021-01-26'\n" +
                    "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1611655200)\n" +
                    "    and \"journaldb\".\"logfile\".\"logdate\" <= date '2021-04-26'\n" +
                    "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1619437701)\n" +
                    "  )\n" +
                    ")";
            Condition cond = conditionWalker.fromString(q);
            Assertions.assertNotNull(cond);
            String result = cond.toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringTimeRanges0ToEpochTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q, e, result;
            q = "<AND><AND><AND><host value=\"sc-99-99-14-25\" operation=\"EQUALS\"/><index value=\"cpu\" operation=\"EQUALS\"/></AND><sourcetype value=\"log:cpu:0\" operation=\"EQUALS\"/></AND><AND><earliest value=\"0\" operation=\"GE\"/><latest value=\"1893491420\" operation=\"LE\"/></AND></AND>";
            e = "(\n" +
                    "  \"getArchivedObjects_filter_table\".\"host\" like 'sc-99-99-14-25'\n" +
                    "  and \"getArchivedObjects_filter_table\".\"directory\" like 'cpu'\n" +
                    "  and \"getArchivedObjects_filter_table\".\"stream\" like 'log:cpu:0'\n" +
                    "  and \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n" +
                    "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n" +
                    "  and \"journaldb\".\"logfile\".\"logdate\" <= date '2030-01-01'\n" +
                    "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1893491420)\n" +
                    ")";
            Condition cond = conditionWalker.fromString(q);
            result = cond.toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringBloomConditionTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
            String eBloom = "(\n" +
                    "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" +
                    "  and (\n" +
                    "    (\n" +
                    "      bloommatch(\n" +
                    "        (\n" +
                    "          select \"term_0_pattern_test_ip\".\"filter\"\n" +
                    "          from \"term_0_pattern_test_ip\"\n" +
                    "          where (\n" +
                    "            term_id = 0\n" +
                    "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" +
                    "          )\n" +
                    "        ),\n" +
                    "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" +
                    "      ) = true\n" +
                    "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" +
                    "    )\n" +
                    "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    "  )\n" +
                    ")";
            String result = conditionWalker.fromString(q).toString();
            Assertions.assertEquals(eBloom, result);
            Assertions.assertEquals(1, conditionWalker.patternMatchTables().size());
        });
    }

    @Test
    void fromStringBloomMultipleTablesTest() {
        Assertions.assertDoesNotThrow(() -> {
            String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/></AND>";
            String e = "(\n" +
                    "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" +
                    "  and (\n" +
                    "    (\n" +
                    "      bloommatch(\n" +
                    "        (\n" +
                    "          select \"term_0_pattern_test_ip\".\"filter\"\n" +
                    "          from \"term_0_pattern_test_ip\"\n" +
                    "          where (\n" +
                    "            term_id = 0\n" +
                    "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" +
                    "          )\n" +
                    "        ),\n" +
                    "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" +
                    "      ) = true\n" +
                    "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" +
                    "    )\n" +
                    "    or (\n" +
                    "      bloommatch(\n" +
                    "        (\n" +
                    "          select \"term_0_pattern_test_ip255\".\"filter\"\n" +
                    "          from \"term_0_pattern_test_ip255\"\n" +
                    "          where (\n" +
                    "            term_id = 0\n" +
                    "            and type_id = \"bloomdb\".\"pattern_test_ip255\".\"filter_type_id\"\n" +
                    "          )\n" +
                    "        ),\n" +
                    "        \"bloomdb\".\"pattern_test_ip255\".\"filter\"\n" +
                    "      ) = true\n" +
                    "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is not null\n" +
                    "    )\n" +
                    "    or (\n" +
                    "      \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is null\n" +
                    "    )\n" +
                    "  )\n" +
                    ")";
            String result = conditionWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
            Assertions.assertEquals(2, conditionWalker.patternMatchTables().size());
        });
    }

    @Test
    void fromStringBloomMultipleSearchTermTest() {
        Assertions.assertDoesNotThrow(() -> {
            // first search term joins one table and second search term joins 2 tables
            String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><AND><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/></AND></AND>";
            String e = "(\n" +
                    "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" +
                    "  and (\n" +
                    "    (\n" +
                    "      bloommatch(\n" +
                    "        (\n" +
                    "          select \"term_0_pattern_test_ip\".\"filter\"\n" +
                    "          from \"term_0_pattern_test_ip\"\n" +
                    "          where (\n" +
                    "            term_id = 0\n" +
                    "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" +
                    "          )\n" +
                    "        ),\n" +
                    "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" +
                    "      ) = true\n" +
                    "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" +
                    "    )\n" +
                    "    or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    "  )\n" +
                    "  and (\n" +
                    "    (\n" +
                    "      bloommatch(\n" +
                    "        (\n" +
                    "          select \"term_1_pattern_test_ip\".\"filter\"\n" +
                    "          from \"term_1_pattern_test_ip\"\n" +
                    "          where (\n" +
                    "            term_id = 1\n" +
                    "            and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" +
                    "          )\n" +
                    "        ),\n" +
                    "        \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" +
                    "      ) = true\n" +
                    "      and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" +
                    "    )\n" +
                    "    or (\n" +
                    "      bloommatch(\n" +
                    "        (\n" +
                    "          select \"term_1_pattern_test_ip255\".\"filter\"\n" +
                    "          from \"term_1_pattern_test_ip255\"\n" +
                    "          where (\n" +
                    "            term_id = 1\n" +
                    "            and type_id = \"bloomdb\".\"pattern_test_ip255\".\"filter_type_id\"\n" +
                    "          )\n" +
                    "        ),\n" +
                    "        \"bloomdb\".\"pattern_test_ip255\".\"filter\"\n" +
                    "      ) = true\n" +
                    "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is not null\n" +
                    "    )\n" +
                    "    or (\n" +
                    "      \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    "      and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is null\n" +
                    "    )\n" +
                    "  )\n" +
                    ")";
            String result = conditionWalker.fromString(q).toString();
            Assertions.assertEquals(e, result);
            Assertions.assertEquals(2, conditionWalker.patternMatchTables().size());
            Assertions.assertTrue(result.contains("term_0_"));
            Assertions.assertTrue(result.contains("term_1_"));
            Assertions.assertFalse(result.contains("term_2_"));
        });
    }

    @Test
    void fromStringBloomWithoutFiltersTest() {
        Assertions.assertDoesNotThrow(() -> {
            DSLContext ctx = DSL.using(conn);
            ConditionWalker walker = new ConditionWalker(ctx, false, true, true);
            String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/></AND>";
            String e = "(\n" +
                    "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" +
                    "  and \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    ")";
            String result = walker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    void fromStringBloomWithoutFilterMultipleSearchTermsTest() {
        Assertions.assertDoesNotThrow(() -> {
            DSLContext ctx = DSL.using(conn);
            ConditionWalker walker = new ConditionWalker(ctx, false, true, true);
            String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><AND><indexstatement operation=\"EQUALS\" value=\"192.168.1.1\"/><indexstatement operation=\"EQUALS\" value=\"255.255.255.255\"/></AND></AND>";
            String e = "(\n" +
                    "  \"getArchivedObjects_filter_table\".\"directory\" like 'haproxy'\n" +
                    "  and \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    "  and \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" +
                    "  and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is null\n" +
                    ")";
            String result = walker.fromString(q).toString();
            Assertions.assertEquals(e, result);
        });
    }

    private void writeFilter(String tableName, int filterId, Connection conn) {
        Assertions.assertDoesNotThrow(() -> {
            String sql = "INSERT INTO `" + tableName + "` (`partition_id`, `filter_type_id`, `filter`) " +
                    "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
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

