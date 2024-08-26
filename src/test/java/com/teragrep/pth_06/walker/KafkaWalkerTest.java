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

import com.teragrep.pth_06.planner.walker.KafkaWalker;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaWalkerTest {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaWalkerTest.class);

    KafkaWalker kafkaWalker;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        kafkaWalker = new KafkaWalker();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        kafkaWalker = null;
    }

    @Test
    void fromStringAndNotEquals() throws Exception {
        String q = "<AND>" + "<index value=\"haproxy\" operation=\"NOT_EQUALS\"/>"
                + "<index value=\"zongprox*\" operation=\"EQUALS\"/>" + "</AND>";
        String e = "^zongprox.*$";
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringOrTest() throws Exception {
        String q = "<OR>" + "<index value=\"nanprox*\" operation=\"EQUALS\"/>"
                + "<index value=\"zongprox*\" operation=\"EQUALS\"/>" + "</OR>";
        String e = "(^nanprox.*$|^zongprox.*$)";
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringOrNotEquals() throws Exception {
        String q = "<OR>" + "<index value=\"nanprox*\" operation=\"NOT_EQUALS\"/>"
                + "<index value=\"zongprox*\" operation=\"EQUALS\"/>" + "</OR>";
        String e = "^zongprox.*$";
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringAnd() throws Exception {
        String q = "<AND><index value=\"nanprox*\" operation=\"EQUALS\"/><index value=\"zongprox*\" operation=\"EQUALS\"/></AND>";
        String e = null;
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringInner() throws Exception {
        // index=nanprox* sourcetype=sauce* OR sourcetype=innersauce* index=innerzong*
        String q = "<OR>" + "<AND>" + "<index value=\"nanprox*\" operation=\"EQUALS\"/>"
                + "<sourcetype value=\"sauce*\" operation=\"EQUALS\"/>" + "</AND>" + "<AND>"
                + "<sourcetype value=\"innersauce*\" operation=\"EQUALS\"/>"
                + "<index value=\"innerzong*\" operation=\"EQUALS\"/>" + "</AND>" + "</OR>";
        String e = "(^nanprox.*$|^innerzong.*$)";
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringOrNot() throws Exception {
        // index=nanprox* sourcetype=sauce* OR sourcetype=innersauce* index=innerzong*
        String q = "<OR>" + "<AND>" + "<index operation=\"EQUALS\" value=\"cpu\"/>"
                + "<sourcetype operation=\"EQUALS\" value=\"log:cpu:0\"/>" + "</AND>" + "<NOT>"
                + "<indexstatement operation=\"EQUALS\" value=\"src\"/>" + "</NOT>" + "</OR>";
        String e = "^cpu$";
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }

    @Test
    void fromStringNot() throws Exception {
        // index=nanprox* sourcetype=sauce* OR sourcetype=innersauce* index=innerzong*
        String q = "<NOT>" + "<indexstatement operation=\"EQUALS\" value=\"src\"/>" + "</NOT>";
        String e = null;
        String result = kafkaWalker.fromString(q);//.toString();
        LOGGER.debug("Query   =" + q);
        LOGGER.debug("Expected=" + e);
        LOGGER.debug("Result  =" + result);
        assertEquals(e, result);
    }
}
