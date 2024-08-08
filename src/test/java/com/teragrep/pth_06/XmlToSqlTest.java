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

package com.teragrep.pth_06;

import com.teragrep.pth_06.planner.walker.ConditionWalker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XmlToSqlTest {
    ConditionWalker streamWalker;
    ConditionWalker walker;

    @BeforeAll
    public static void setUp() {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
    }

    @BeforeEach
    void befeoreEach() {
        walker = new ConditionWalker(null, false);
        streamWalker = new ConditionWalker(null, true);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        walker = null;
    }

    @Test
    void fromStringTest_StreamDB_NotIndex() {
        String q = "<index value=\"haproxy\" operation=\"NOT_EQUALS\"/>";
        String e = "not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')";
        String result = streamWalker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_AndSourcetype() {
        String q;
        String e;
        String result;
        q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND>";
        e = "(\n" +
                "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n" +
                "  and \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                ")";
        result = streamWalker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_NotIndexOrSourcetype() {
        String q;
        String e;
        String result;
        q = "<OR><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        e = "(\n" +
                "  not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n" +
                "  or \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                ")";
        result = streamWalker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_IndexOrSourcetype() {
        String q;
        String e;
        String result;
        q = "<OR><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        result = streamWalker.fromString(q).toString();
        q = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
        e = "(\n" +
                "  \"streamdb\".\"stream\".\"directory\" like '%'\n" +
                "  or (\n" +
                "    \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n" +
                "    and \"streamdb\".\"stream\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                "  )\n" +
                ")";
        result = streamWalker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_StreamDB_DropIndexStatementAndEarliest() {
        String q;
        String e;
        String result;
        // Drop indexstatement and earliest from query
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
        e = "(\n" +
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
        result = streamWalker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_OrTimestampGTE() {
        String q;
        String e;
        String result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><earliest value=\"1611612000\" operation=\"GE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(1611612000);
        Date fromDate = new Date(fromTime.toEpochMilli());
        e = "(\n" +
                "  (\n" +
                "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                "    and \"getArchivedObjects_filter_table\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'zong.xnet.fi'\n" +
                "  )\n" +
                "  or (\n" +
                "    \"getArchivedObjects_filter_table\".\"directory\" like '%'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'salengar.xnet.fi'\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" >= date '" + fromDate + "'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= " + fromTime.getEpochSecond() + ")\n" +
                "  )\n" +
                ")";
        result = walker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_OrTimestampLTE() {
        String q;
        String e;
        String result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><latest value=\"1611611999\" operation=\"LE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(1611611999);
        Date fromDate = new Date(fromTime.toEpochMilli());
        e = "(\n" +
                "  (\n" +
                "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                "    and \"getArchivedObjects_filter_table\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'zong.xnet.fi'\n" +
                "  )\n" +
                "  or (\n" +
                "    \"getArchivedObjects_filter_table\".\"directory\" like '%'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'salengar.xnet.fi'\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" <= date '" + fromDate + "'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= " + fromTime.getEpochSecond() + ")\n" +
                "  )\n" +
                ")";
        result = walker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_OrTimestampBetween() {
        String q;
        String e;
        String result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"xnet:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"zong.xnet.fi\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"salengar.xnet.fi\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstatement value=\"Denied\" /></AND></OR>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(1611655200);
        Date fromDate = new Date(fromTime.toEpochMilli());
        Instant toTime = Instant.ofEpochSecond(1619437701);
        Date toDate = new Date(toTime.toEpochMilli());

        e = "(\n" +
                "  (\n" +
                "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                "    and \"getArchivedObjects_filter_table\".\"stream\" like 'xnet:haproxy:haproxy'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'zong.xnet.fi'\n" +
                "  )\n" +
                "  or (\n" +
                "    \"getArchivedObjects_filter_table\".\"directory\" like '%'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'salengar.xnet.fi'\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" >= date '" + fromDate + "'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= " + fromTime.getEpochSecond() + ")\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" <= date '" + toDate + "'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= " + toTime.getEpochSecond() + ")\n" +
                "  )\n" +
                ")";
        result = walker.fromString(q).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTest_JournalDB_AndTimestampBetween() {
        String q;
        String e;
        String result;
        q = "<AND><AND><AND><host value=\"sc-99-99-14-25\" operation=\"EQUALS\"/><index value=\"cpu\" operation=\"EQUALS\"/></AND><sourcetype value=\"log:cpu:0\" operation=\"EQUALS\"/></AND><AND><earliest value=\"0\" operation=\"GE\"/><latest value=\"1893491420\" operation=\"LE\"/></AND></AND>";
        // Add time ranges
        Instant fromTime = Instant.ofEpochSecond(0);
        Date fromDate = new Date(fromTime.toEpochMilli());
        Instant toTime = Instant.ofEpochSecond(1893491420);
        Date toDate = new Date(toTime.toEpochMilli());
        e = "(\n" +
                "  \"getArchivedObjects_filter_table\".\"host\" like 'sc-99-99-14-25'\n" +
                "  and \"getArchivedObjects_filter_table\".\"directory\" like 'cpu'\n" +
                "  and \"getArchivedObjects_filter_table\".\"stream\" like 'log:cpu:0'\n" +
                "  and \"journaldb\".\"logfile\".\"logdate\" >= date '" + fromDate + "'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= " + fromTime.getEpochSecond() + ")\n" +
                "  and \"journaldb\".\"logfile\".\"logdate\" <= date '" + toDate + "'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= " + toTime.getEpochSecond() + ")\n" +
                ")";
        result = walker.fromString(q).toString();
        assertEquals(e, result);
    }
}
