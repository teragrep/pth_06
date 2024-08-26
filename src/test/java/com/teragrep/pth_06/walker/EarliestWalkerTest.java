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

import com.teragrep.pth_06.planner.walker.EarliestWalker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EarliestWalkerTest {

    EarliestWalker earliestWalker;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        earliestWalker = new EarliestWalker();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        earliestWalker = null;
    }

    @Test
    void earliestTest() throws Exception {
        String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        Long result = earliestWalker.fromString(q);

        assertEquals(1611657303L, result);
    }

    @Test
    void indexEarliestTest() throws Exception {
        String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><index_earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        Long result = earliestWalker.fromString(q);

        assertEquals(1611657303L, result);
    }

    @Test
    void globalEarliestTest() throws Exception {
        String q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"2611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        Long result = earliestWalker.fromString(q);
        Long global = earliestWalker.globalEarliestEpoch;

        assertEquals(global, result);
    }

    @Test
    void compareEarliestTEst() throws Exception {
        String q = "<OR><earliest value=\"2\" operation=\"GE\"/><earliest value=\"1\" operation=\"GE\"/></OR>";
        Long result = earliestWalker.fromString(q);

        assertEquals(1L, result);
    }

    @Test
    void globalEarliestWithCompareTest() throws Exception {
        String q = "<OR><earliest value=\"2611657303\" operation=\"GE\"/><earliest value=\"2611657302\" operation=\"GE\"/></OR>";
        Long result = earliestWalker.fromString(q);
        Long global = earliestWalker.globalEarliestEpoch;

        assertEquals(global, result);
    }

    @Test
    void ignoreUnaryOperationsTest() throws Exception {
        String q = "<AND><OR><earliest value=\"2611657302\" operation=\"GE\"/><earliest value=\"1000\" operation=\"GE\"/></OR><NOT><earliest value=\"1\" operation=\"GE\"/></NOT></AND>";
        Long result = earliestWalker.fromString(q);
        Long global = earliestWalker.globalEarliestEpoch;

        assertEquals(1000L, result);
    }

    @Test
    void ignoreUnaryOperationsWithGlobalTest() throws Exception {
        String q = "<AND><OR><earliest value=\"2611657302\" operation=\"GE\"/><earliest value=\"2611657304\" operation=\"GE\"/></OR><NOT><earliest value=\"1\" operation=\"GE\"/></NOT></AND>";
        Long result = earliestWalker.fromString(q);
        Long global = earliestWalker.globalEarliestEpoch;

        assertEquals(global, result);
    }
}
