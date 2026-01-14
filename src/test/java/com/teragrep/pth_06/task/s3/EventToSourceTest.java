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
package com.teragrep.pth_06.task.s3;

import com.teragrep.rlo_06.RFC5424Frame;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

final class EventToSourceTest {

    @Test
    void testSourceFromEventNode() {
        final String syslog = "<34>1 2024-01-01T00:00:00Z host app 1234 - "
                + "[event_node_source@48577 source_module=\"imudp\" hostname=\"src.host\" source=\"10.0.0.1\"] "
                + "test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertTrue(frame.next());
        });

        final EventToSource eventToSource = new EventToSource();
        final UTF8String result = eventToSource.asUTF8StringFrom(frame);
        Assertions.assertEquals(UTF8String.fromString("imudp:src.host:10.0.0.1"), result);
    }

    @Test
    void testFallbackToRelayWhenSourceMissing() {
        final String syslog = "<34>1 2024-01-01T00:00:00Z host app 1234 - "
                + "[event_node_relay@48577 source_module=\"imrelp\" hostname=\"relay.host\" source=\"192.168.1.10\"] "
                + "test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertTrue(frame.next());
        });
        final EventToSource eventToSource = new EventToSource();
        final UTF8String result = eventToSource.asUTF8StringFrom(frame);
        Assertions.assertEquals(UTF8String.fromString("imrelp:relay.host:192.168.1.10"), result);
    }

    @Test
    void testNoSourceOrRelayPresent() {
        final String syslog = "<34>1 2024-01-01T00:00:00Z host app 1234 - - test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertTrue(frame.next());
        });
        final EventToSource eventToSource = new EventToSource();
        final UTF8String result = eventToSource.asUTF8StringFrom(frame);
        Assertions.assertEquals(UTF8String.fromString("::"), result);
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EventToSource.class).verify();
    }
}
