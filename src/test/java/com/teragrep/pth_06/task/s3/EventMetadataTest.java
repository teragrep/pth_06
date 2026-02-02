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
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class EventMetadataTest {

    @Test
    void testSyslogFrameJson() {
        final String path = "2024/01-01/sc-99-99-14-110/f17/f17.log";
        final String syslog = "<34>1 2024-01-01T05:00:00Z host app 1234 ID47 - test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(frame::next);
        final EventMetadata metadata = new EventMetadata("bucket", path, "partition-1");
        final JsonObject json = metadata.toJSONWithSyslogTimestamp(frame);
        Assertions.assertTrue(json.getBoolean("epochMigration"));
        Assertions.assertEquals("rfc5424", json.getString("format"));

        final JsonObject object = json.getJsonObject("object");
        Assertions.assertEquals("bucket", object.getString("bucket"));
        Assertions.assertEquals("2024/01-01/sc-99-99-14-110/f17/f17.log", object.getString("path"));
        Assertions.assertEquals("partition-1", object.getString("partition"));

        final JsonObject timestampJson = json.getJsonObject("timestamp");
        Assertions.assertEquals("syslog", timestampJson.getString("source"));
        final long expectedEpochMicros = ZonedDateTime.of(2024, 1, 1, 5, 0, 0, 0, ZoneId.of("UTC")).toEpochSecond()
                * 1000 * 1000;
        Assertions.assertEquals(expectedEpochMicros, timestampJson.getJsonNumber("epoch").longValue());
        final ZonedDateTime expectedPathDerived = ZonedDateTime
                .of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Helsinki"));
        Assertions.assertEquals(expectedPathDerived, ZonedDateTime.parse(timestampJson.getString("path-extracted")));
        Assertions.assertEquals(frame.timestamp.toString(), timestampJson.getString("rfc5242timestamp"));

    }

    @Test
    void testNonSyslogFrameJson() {
        final String path = "2007/10-08/sc-99-99-14-110/f17/f17.log";
        final EventMetadata metadata = new EventMetadata("bucket", path, "partition-2");
        final JsonObject json = metadata.toJSONWithPathExtractedTimestamp();
        Assertions.assertTrue(json.getBoolean("epochMigration"));
        Assertions.assertEquals("non-rfc5424", json.getString("format"));

        final JsonObject object = json.getJsonObject("object");
        Assertions.assertEquals("bucket", object.getString("bucket"));
        Assertions.assertEquals("2007/10-08/sc-99-99-14-110/f17/f17.log", object.getString("path"));
        Assertions.assertEquals("partition-2", object.getString("partition"));

        final JsonObject timestampJson = json.getJsonObject("timestamp");
        Assertions.assertEquals("object-path", timestampJson.getString("source"));
        final ZonedDateTime expectedPathExtracted = ZonedDateTime
                .of(2007, 10, 8, 0, 0, 0, 0, ZoneId.of("Europe/Helsinki"));
        Assertions.assertEquals(expectedPathExtracted, ZonedDateTime.parse(timestampJson.getString("path-extracted")));
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EventMetadata.class).verify();
    }
}
