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
import com.teragrep.rlo_06.RFC5424Timestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public final class EpochMicrosTest {

    @Test
    void testAsLongValue() {
        final String syslog = "<34>1 2024-01-01T00:00:00Z host app 1234 - - test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(() -> Assertions.assertTrue(frame.next()));
        final EpochMicros epochMicros = new EpochMicros(new RFC5424Timestamp(frame.timestamp));
        final ZonedDateTime zdt = ZonedDateTime.parse("2024-01-01T00:00:00Z", DateTimeFormatter.ISO_ZONED_DATE_TIME);
        final long expected = zdt.toEpochSecond() * 1_000_000L + zdt.getNano() / 1_000L;
        final long actual = epochMicros.asLong();
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testCorrectConversionOfNanoSeconds() {
        final String syslog = "<34>1 2024-01-01T00:00:00.123456789Z host app 1234 - - test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(() -> Assertions.assertTrue(frame.next()));
        final EpochMicros epochMicros = new EpochMicros(new RFC5424Timestamp(frame.timestamp));
        final ZonedDateTime zdt = ZonedDateTime
                .parse("2024-01-01T00:00:00.123456789Z", DateTimeFormatter.ISO_ZONED_DATE_TIME);
        final long expected = zdt.toEpochSecond() * 1_000_000L + zdt.getNano() / 1_000L; // nanos -> micros
        final long actual = epochMicros.asLong();
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EpochMicros.class).verify();
    }
}
