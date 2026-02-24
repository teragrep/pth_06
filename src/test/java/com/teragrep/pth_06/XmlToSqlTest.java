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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class XmlToSqlTest {

    @Test
    public void testStreamDBQueryIndexNotEquals() {
        final String query = "<index value=\"haproxy\" operation=\"NOT_EQUALS\"/>";
        final String expected = "not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')";
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, true).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testStreamDBQueryIndexAndSourceType() {
        final String query = "<AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND>";
        final String expected = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, true).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testStreamDBQueryNotIndexOrSourcetype() {
        final String query = "<OR><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></OR>";
        final String expected = "(\n" + "  not (\"streamdb\".\"stream\".\"directory\" like 'haproxy')\n"
                + "  or \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + ")";
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, true).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testStreamDBIndexOrSourceType() {
        final String query = "<OR><index value=\"*\" operation=\"EQUALS\"/><AND><index value=\"haproxy\" operation=\"EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND></OR>";
        final String expected = "(\n" + "  true\n" + "  or (\n"
                + "    \"streamdb\".\"stream\".\"directory\" like 'haproxy'\n"
                + "    and \"streamdb\".\"stream\".\"stream\" like 'example:haproxy:haproxy'\n" + "  )\n" + ")";
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, true).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testJournalDBQueryTimestampBetweenGreaterThanOrEqual() {
        final String query = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611612000\" operation=\"GE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        // spotless:off
        final String expected = "(\n" +
                "  (\n" +
                "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" +
                "  )\n" +
                "  or (\n" +
                "    true\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" >= date '2021-01-26'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1611612000)\n" +
                "  )\n" +
                ")";
        // spotless:on
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, false).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testJournalDBQueryOrTimestampLessThanEquals() {
        final String query = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><latest value=\"1611611999\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        final String expected = "(\n" + "  (\n"
                + "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n"
                + "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" + "  )\n"
                + "  or (\n" + "    true\n"
                + "    and \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n"
                + "    and \"journaldb\".\"logfile\".\"logdate\" <= date '2021-01-25'\n"
                + "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1611611999)\n"
                + "  )\n" + ")";
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, false).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testJournalDBQueryOrTimestampBetween() {
        final String query = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        // spotless:off
        final String expected = "(\n" +
                "  (\n" +
                "    not (\"getArchivedObjects_filter_table\".\"directory\" like 'haproxy')\n" +
                "    and \"getArchivedObjects_filter_table\".\"stream\" like 'example:haproxy:haproxy'\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'loadbalancer.example.com'\n" +
                "  )\n" +
                "  or (\n" +
                "    true\n" +
                "    and \"getArchivedObjects_filter_table\".\"host\" like 'firewall.example.com'\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" >= date '2021-01-26'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 1611655200)\n" +
                "    and \"journaldb\".\"logfile\".\"logdate\" <= date '2021-04-26'\n" +
                "    and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1619437701)\n" +
                "  )\n" +
                ")";
        // spotless:on
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, false).toString());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testJournalDBQueryAndTimestampBetween() {
        final String query = "<AND><AND><AND><host value=\"sc-99-99-14-25\" operation=\"EQUALS\"/><index value=\"cpu\" operation=\"EQUALS\"/></AND><sourcetype value=\"log:cpu:0\" operation=\"EQUALS\"/></AND><AND><earliest value=\"0\" operation=\"GE\"/><latest value=\"1893491420\" operation=\"LE\"/></AND></AND>";
        // spotless:off
        final String expected = "(\n" +
                "  \"getArchivedObjects_filter_table\".\"host\" like 'sc-99-99-14-25'\n" +
                "  and \"getArchivedObjects_filter_table\".\"directory\" like 'cpu'\n" +
                "  and \"getArchivedObjects_filter_table\".\"stream\" like 'log:cpu:0'\n" +
                "  and \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n" +
                "  and \"journaldb\".\"logfile\".\"logdate\" <= date '2030-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1893491420)\n" +
                ")";
        // spotless:on
        final String result = Assertions
                .assertDoesNotThrow(() -> new ConditionWalker().fromString(query, false).toString());
        Assertions.assertEquals(expected, result);
    }
}
