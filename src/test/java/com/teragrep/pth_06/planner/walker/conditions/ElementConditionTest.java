/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2024  Suomen Kanuuna Oy
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

package com.teragrep.pth_06.planner.walker.conditions;

import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ElementConditionTest {
    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    Document document;
    ConditionConfig streamConfig;
    ConditionConfig bloomConfig;
    Connection conn;
    List<String> patternList = new ArrayList<>(Arrays.asList(
            "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}",
            "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}"
    ));

    @BeforeAll
    void setUp() {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
        Assertions.assertDoesNotThrow(() -> {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            this.document = factory.newDocumentBuilder().newDocument();
            final Connection conn = DriverManager.getConnection(url, userName, password);

            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS filtertype").execute();
            String filtertype = "CREATE TABLE`filtertype`" +
                    "(" +
                    "    `id`               bigint(20) unsigned   NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `expectedElements` bigint(20) unsigned NOT NULL," +
                    "    `targetFpp`        DOUBLE(2) unsigned NOT NULL," +
                    "    `pattern`          VARCHAR(2048) NOT NULL," +
                    "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`)" +
                    ")";
            conn.prepareStatement(filtertype).execute();
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
            this.conn = conn;
            final DSLContext ctx = DSL.using(conn);
            this.streamConfig = new ConditionConfig(ctx, true);
            this.bloomConfig = new ConditionConfig(ctx, false, true, false);
        });
    }

    @BeforeEach
    void createTargetTable() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP TABLE IF EXISTS target").execute();
            String targetTable = "CREATE TABLE `target`(" +
                    "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE," +
                    "    `filter_type_id` bigint(20) unsigned NOT NULL," +
                    "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(targetTable).execute();
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
    void testXML() {
        Element element = document.createElement("element");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "AND");
        Assertions.assertEquals("element", element.getTagName());
        Assertions.assertEquals("f17", element.getAttribute("value"));
        Assertions.assertEquals("AND", element.getAttribute("operation"));
    }

    @Test
    public void indexConditionTest() {
        Element element = document.createElement("index");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        ElementCondition streamElementCondition = new ElementCondition(element, streamConfig, 0);
        String eBloom = "\"getArchivedObjects_filter_table\".\"directory\" like 'f17'";
        String eStream = "\"streamdb\".\"stream\".\"directory\" like 'f17'";
        Assertions.assertEquals(eBloom, elementCondition.condition().toString());
        Assertions.assertEquals(eStream, streamElementCondition.condition().toString());
    }

    @Test
    public void notEqualsTest() {
        Element element = document.createElement("index");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "NOT_EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        ElementCondition streamElementCondition = new ElementCondition(element, streamConfig, 0);
        String eBloom = "not (\"getArchivedObjects_filter_table\".\"directory\" like 'f17')";
        String eStream = "not (\"streamdb\".\"stream\".\"directory\" like 'f17')";
        Assertions.assertEquals(eBloom, elementCondition.condition().toString());
        Assertions.assertEquals(eStream, streamElementCondition.condition().toString());
    }

    @Test
    public void sourceTypeConditionTest() {
        Element element = document.createElement("sourcetype");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        ElementCondition streamElementCondition = new ElementCondition(element, streamConfig, 0);
        String eBloom = "\"getArchivedObjects_filter_table\".\"stream\" like 'f17'";
        String eStream = "\"streamdb\".\"stream\".\"stream\" like 'f17'";
        Assertions.assertEquals(eBloom, elementCondition.condition().toString());
        Assertions.assertEquals(eStream, streamElementCondition.condition().toString());
    }

    @Test
    public void hostTypeConditionTest() {
        Element element = document.createElement("host");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        ElementCondition streamElementCondition = new ElementCondition(element, streamConfig, 0);
        String eBloom = "\"getArchivedObjects_filter_table\".\"host\" like 'f17'";
        String eStream = "\"streamdb\".\"host\".\"name\" like 'f17'";
        Assertions.assertEquals(eBloom, elementCondition.condition().toString());
        Assertions.assertEquals(eStream, streamElementCondition.condition().toString());
    }

    @Test
    public void earliestTypeConditionTest() {
        Element element = document.createElement("earliest");
        element.setAttribute("value", "1000");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n" +
                ")";
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        Assertions.assertEquals(1000L, elementCondition.earliest());
        Assertions.assertEquals(e, elementCondition.condition().toString());
    }

    @Test
    public void earliestIsUpdatedTest() {
        Element element = document.createElement("earliest");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" >= date '2000-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 946677600)\n" +
                ")";
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        Assertions.assertEquals(946677600L, elementCondition.earliest());
        Assertions.assertEquals(e, elementCondition.condition().toString());
    }

    @Test
    public void latestTypeTest() {
        Element element = document.createElement("latest");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '2000-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 946677600)\n" +
                ")";
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        Assertions.assertEquals(e, elementCondition.condition().toString());
    }

    @Test
    public void indexStatementTypeTest() {
        fillTargetTable();
        Element element = document.createElement("indexstatement");
        element.setAttribute("value", "192.124.0.0");
        element.setAttribute("operation", "EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        String e = "(\n" +
                "  (\n" +
                "    bloommatch(\n" +
                "      (\n" +
                "        select \"term_0_target\".\"filter\"\n" +
                "        from \"term_0_target\"\n" +
                "        where (\n" +
                "          term_id = 0\n" +
                "          and type_id = \"bloomdb\".\"target\".\"filter_type_id\"\n" +
                "        )\n" +
                "      ),\n" +
                "      \"bloomdb\".\"target\".\"filter\"\n" +
                "    ) = true\n" +
                "    and \"bloomdb\".\"target\".\"filter\" is not null\n" +
                "  )\n" +
                "  or \"bloomdb\".\"target\".\"filter\" is null\n" +
                ")";
        Assertions.assertTrue(elementCondition.isIndexStatement());
        Assertions.assertEquals(e, elementCondition.condition().toString());
        Assertions.assertFalse(elementCondition.matchList().isEmpty());
    }

    @Test
    public void IndexStatementAgainstEmptyBloomTableTest() {
        Element element = document.createElement("indexstatement");
        element.setAttribute("value", "192.124.0.0");
        element.setAttribute("operation", "EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, bloomConfig, 0);
        Assertions.assertNull(elementCondition.condition());
        Assertions.assertTrue(elementCondition.isIndexStatement());
        Assertions.assertTrue(elementCondition.matchList().isEmpty());
    }

    private void fillTargetTable() {
        Assertions.assertDoesNotThrow(() -> {
            String sql = "INSERT INTO `target` (`partition_id`, `filter_type_id`, `filter`) " +
                    "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            BloomFilter filter = BloomFilter.create(1000, 0.01);
            final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                filter.writeTo(filterBAOS);
                filterBAOS.close();
            });
            stmt.setInt(1, 1);
            stmt.setInt(2, 1);
            stmt.setBytes(3, filterBAOS.toByteArray());
            stmt.executeUpdate();
        });
    }
}