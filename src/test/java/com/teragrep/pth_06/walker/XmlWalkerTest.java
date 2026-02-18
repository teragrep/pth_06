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
import com.teragrep.pth_06.planner.walker.PlainWalker;
import org.jooq.Condition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XmlWalkerTest {

    private final Logger LOGGER = LoggerFactory.getLogger(XmlWalkerTest.class);

    ConditionWalker conditionWalker;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        conditionWalker = new ConditionWalker();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        conditionWalker = null;
    }

    @Test
    void fromStringIntendTest() throws Exception {
        PlainWalker walker = new PlainWalker();
        String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"2021-01-21T11:58:37\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        walker.fromString(q);
        LOGGER.debug("---------------");
        q = "<AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND>";
        walker.fromString(q);
        LOGGER.debug("---------------");
    }

    @Test
    void fromStringTest() throws Exception {
        String q = "<index value=\"haproxy\" operation=\"NOT_EQUALS\"/>";
        String e = "not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')";
        String result = conditionWalker.fromString(q, true).toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringAndTest() throws Exception {
        String q = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND>";
        String e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";

        Condition cond = conditionWalker.fromString(q, true);
        LOGGER.debug("ConditionWalkerResult=" + cond.toString());

        String result = cond.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringOrNETest() throws Exception {
        String q, e, result;
        q = "<OR><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        e = "(\n" + "  not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n"
                + "  or \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        result = conditionWalker.fromString(q, true).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringNotTest() throws Exception {
        String q, e, result;
        // index=cpu sourcetype=log:cpu:0 NOT src
        q = "<OR><index value=\"cpu\" operation=\"EQUALS\"/><sourcetype value=\"log:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'cpu'\n"
                + "  or \"streamdb\".\"stream\".\"stream\" like 'log:haproxy:haproxy'\n" + ")";
        result = conditionWalker.fromString(q, true).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringNot1Test() throws Exception {
        String q, e, result;
        q = "<AND><AND><index operation=\"EQUALS\" value=\"cpu\"/><sourcetype operation=\"EQUALS\" value=\"log:cpu:0\"/></AND><NOT><indexstatement operation=\"EQUALS\" value=\"src\"/></NOT></AND>";
        e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'cpu'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'log:cpu:0'\n" + ")";
        result = conditionWalker.fromString(q, true).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringOrAndTest() throws Exception {
        String q, e, result;
        q = "<OR><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        result = conditionWalker.fromString(q, true).toString();
        LOGGER.debug("Query=" + q);
        LOGGER.debug("Result=" + result);
        LOGGER.debug("---------------");
        q = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
        e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        result = conditionWalker.fromString(q, true).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringOrAnd1Test() throws Exception {
        String q, e, result;
        q = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
        e = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        result = conditionWalker.fromString(q, true).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringDropTimesTest() throws Exception {
        String q, e, result;
        // Drop indexstring and earliest from query
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        e = "(\n" + "  (\n" + "    not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n"
                + "    and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"streamdb\".\"host\".\"name\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or \"streamdb\".\"host\".\"name\" like 'firewall.example.com'\n" + ")";
        result = conditionWalker.fromString(q, true).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTimeRangesTest() throws Exception {
        String q, e, result;
        // Drop indexstring and earliest from query
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        e = "(\n" + "  (\n" + "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n"
                + "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or (\n" + "    \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" >= date '2021-01-26'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1611655200)\n"
                + "  )\n" + ")";
        result = conditionWalker.fromString(q, false).toString();
        assertEquals(e, result);
    }

    @Test
    void fromStringTimeRangesUsingEpochTest() throws Exception {
        String q, e, result;
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        e = "(\n" + "  (\n" + "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n"
                + "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or (\n" + "    \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" >= date '2021-01-26'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1611655200)\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" <= date '2021-04-26'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1619437701)\n"
                + "  )\n" + ")";
        Condition cond = conditionWalker.fromString(q, false);
        if (cond != null) {
            result = conditionWalker.fromString(q, false).toString();
        }
        else {
            result = "illegal null condition";
        }
        assertEquals(e, result);
    }

    @Test
    void fromStringTimeRanges0ToEpochTest() throws Exception {
        String q, e, result;
        q = "<AND><AND><AND><host value=\"sc-99-99-14-25\" operation=\"EQUALS\"/><index value=\"cpu\" operation=\"EQUALS\"/></AND><sourcetype value=\"log:cpu:0\" operation=\"EQUALS\"/></AND><AND><earliest value=\"0\" operation=\"GE\"/><latest value=\"1893491420\" operation=\"LE\"/></AND></AND>";
        e = "(\n" + "  \"getArchivedObjects_filter_table\".\"host\" like 'sc-99-99-14-25'\n"
                + "  and \"getArchivedObjects_filter_table\".\"directory\" like 'cpu'\n"
                + "  and \"getArchivedObjects_filter_table\".\"stream\" like 'log:cpu:0'\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n"
                + "  and \"journaldb\".\"logfile\".\"logdate\" <= date '2030-01-01'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1893491420)\n"
                + ")";
        Condition cond = conditionWalker.fromString(q, false);
        result = cond.toString();
        assertEquals(e, result);
    }
}
