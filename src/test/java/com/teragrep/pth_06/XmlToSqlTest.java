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

import com.teragrep.pth_06.planner.walker.ConditionWalker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XmlToSqlTest {

    private final Logger LOGGER = LoggerFactory.getLogger(XmlToSqlTest.class);

    ConditionWalker parser;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        parser = new ConditionWalker();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        parser = null;
    }

    @Test
    void fromStringTest_StreamDB_NotIndex() throws Exception {
        String q = "<index value=\"haproxy\" operation=\"NOT_EQUALS\"/>";
        String e = "not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')";
        String result = parser.fromString(q, true).toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_AndSourcetype() throws Exception {
        String q;
        String e;
        String result;
        LOGGER.debug("------ AND ---------");
        q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND>";
        e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        result = parser.fromString(q, true).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_NotIndexOrSourcetype() throws Exception {
        String q;
        String e;
        String result;
        LOGGER.debug("------ OR ---------");
        q = "<OR><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        e = "(\n" + "  not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n"
                + "  or \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        result = parser.fromString(q, true).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_IndexOrSourcetype() throws Exception {
        String q;
        String e;
        String result;
        LOGGER.debug("---------------");
        q = "<OR><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        result = parser.fromString(q, true).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Result=" + result);
        LOGGER.debug("---------------");
        q = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
        e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        result = parser.fromString(q, true).toString();
        assertEquals(e, result);
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
    }

    @Test
    void fromStringTest_StreamDB_DropIndexStringAndEarliest() throws Exception {
        String q;
        String e;
        String result;
        LOGGER.debug("---------------");
        // Drop indexstring and earliest from query
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        e = "(\n" + "  (\n" + "    not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n"
                + "    and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"streamdb\".\"host\".\"name\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or \"streamdb\".\"host\".\"name\" like 'firewall.example.com'\n" + ")";
        result = parser.fromString(q, true).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_OrTimestampGTE() throws Exception {
        String q;
        String e;
        String result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611612000\" operation=\"GE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(1611612000);
        Date fromDate = new Date(fromTime.toEpochMilli());
        LOGGER.debug("Journal-DB");
        e = "(\n" + "  (\n" + "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n"
                + "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or (\n" + "    \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" >= date '" + fromDate + "'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= "
                + fromTime.getEpochSecond() + ")\n" + "  )\n" + ")";
        result = parser.fromString(q, false).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_OrTimestampLTE() throws Exception {
        String q;
        String e;
        String result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><latest value=\"1611611999\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(1611611999);
        Date fromDate = new Date(fromTime.toEpochMilli());
        LOGGER.debug("Journal-DB");
        e = "(\n" + "  (\n" + "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n"
                + "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or (\n" + "    \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" <= date '" + fromDate + "'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= "
                + fromTime.getEpochSecond() + ")\n" + "  )\n" + ")";
        result = parser.fromString(q, false).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_OrTimestampBetween() throws Exception {
        String q;
        String e;
        String result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(1611655200);
        Date fromDate = new Date(fromTime.toEpochMilli());
        Instant toTime = Instant.ofEpochSecond(1619437701);
        Date toDate = new Date(toTime.toEpochMilli());

        e = "(\n" + "  (\n" + "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n"
                + "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or (\n" + "    \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" >= date '" + fromDate + "'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= "
                + fromTime.getEpochSecond() + ")\n" + "    and \"journaldb\".\"logfile\".\"logdate\" <= date '" + toDate
                + "'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= "
                + toTime.getEpochSecond() + ")\n" + "  )\n" + ")";
        result = parser.fromString(q, false).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_AndTimestampBetween() throws Exception {
        String q;
        String e;
        String result;
        q = "<AND><AND><AND><host value=\"sc-99-99-14-25\" operation=\"EQUALS\"/><index value=\"cpu\" operation=\"EQUALS\"/></AND><sourcetype value=\"log:cpu:0\" operation=\"EQUALS\"/></AND><AND><earliest value=\"0\" operation=\"GE\"/><latest value=\"1893491420\" operation=\"LE\"/></AND></AND>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(0);
        Date fromDate = new Date(fromTime.toEpochMilli());
        Instant toTime = Instant.ofEpochSecond(1893491420);
        Date toDate = new Date(toTime.toEpochMilli());

        e = "(\n" + "  \"getArchivedObjects_filter_table\".\"host\" like 'sc-99-99-14-25'\n"
                + "  and \"getArchivedObjects_filter_table\".\"directory\" like 'cpu'\n"
                + "  and \"getArchivedObjects_filter_table\".\"stream\" like 'log:cpu:0'\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" >= date '" + fromDate + "'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= "
                + fromTime.getEpochSecond() + ")\n" + "  and \"journaldb\".\"logfile\".\"logdate\" <= date '" + toDate
                + "'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= "
                + toTime.getEpochSecond() + ")\n" + ")";
        result = parser.fromString(q, false).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result=" + result);
        assertEquals(e, result);
    }

    @Test
    @Disabled
    void fromStringTest_BloomDB_indexStatement() throws Exception {
        System.out.println("------ BLOOMDB ------");
        String q;
        String e;
        String result;
        q = "<AND><index operation=\"EQUALS\" value=\"haproxy\"/><indexstatement operation=\"EQUALS\" value=\"keyWord\"/></AND>";

        result = parser.fromString(q, false).toString();

    }

    //@org.junit.jupiter.api.Test
    void escapeSpecialCharactersTest() throws Exception {
        String q = "*";
        String e = "%";
        String result = parser.escapeSpecialCharacters(q);
        LOGGER.debug("String   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
        q = "den%ied";
        e = "den\\%ied";
        result = parser.escapeSpecialCharacters(q);
        LOGGER.debug("String   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
        q = "den%*ied";
        e = "den\\%\\*ied";
        result = parser.escapeSpecialCharacters(q);
        LOGGER.debug("String   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);

    }

}
