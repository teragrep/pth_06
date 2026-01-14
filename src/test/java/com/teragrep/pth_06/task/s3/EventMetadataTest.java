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
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public final class EventMetadataTest {

    @Test
    void testSyslogFrameJson() {
        final String syslog = "<34>1 2024-01-01T00:00:00Z host app 1234 ID47 - test message";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(syslog.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertDoesNotThrow(frame::next);
        final RFC5424Timestamp timestamp = new RFC5424Timestamp(frame.timestamp);
        final EpochMicros epochMicros = new EpochMicros(timestamp);
        final EventMetadata metadata = new EventMetadata("bucket", "path/file.gz", "partition-1");
        final JsonObject json = metadata.asJSON(frame, epochMicros, 1L, true);
        Assertions.assertEquals(true, json.getBoolean("epochMigration"));
        Assertions.assertEquals("rfc5424", json.getString("format"));

        final JsonObject object = json.getJsonObject("object");
        Assertions.assertEquals("bucket", object.getString("bucket"));
        Assertions.assertEquals("path/file.gz", object.getString("path"));
        Assertions.assertEquals(1L, object.getJsonNumber("offset").longValue());
        Assertions.assertEquals("partition-1", object.getString("partition"));

        final JsonObject timestampJson = json.getJsonObject("timestamp");
        Assertions.assertEquals("syslog", timestampJson.getString("source"));
        Assertions.assertEquals(epochMicros.asLong(), timestampJson.getJsonNumber("epoch").longValue());
        Assertions.assertEquals(frame.timestamp.toString(), json.getJsonObject("timestamp").getString("original"));

    }

    @Test
    void testNonSyslogFrameJson() {
        final String nonSyslog = "non syslog event";
        final RFC5424Frame frame = new RFC5424Frame(true);
        frame.load(new ByteArrayInputStream(nonSyslog.getBytes(StandardCharsets.UTF_8)));
        final RFC5424Timestamp timestamp = new RFC5424Timestamp(frame.timestamp);
        final EpochMicros epochMicros = new EpochMicros(timestamp);
        final EventMetadata metadata = new EventMetadata("bucket", "path/file.gz", "partition-2");
        final JsonObject json = metadata.asJSON(frame, epochMicros, 0L, false);
        Assertions.assertEquals(true, json.getBoolean("epochMigration"));
        Assertions.assertEquals("non-rfc5424", json.getString("format"));

        final JsonObject object = json.getJsonObject("object");
        Assertions.assertEquals("bucket", object.getString("bucket"));
        Assertions.assertEquals("path/file.gz", object.getString("path"));
        Assertions.assertEquals(0L, object.getJsonNumber("offset").longValue());
        Assertions.assertEquals("partition-2", object.getString("partition"));

        final JsonObject timestampJson = json.getJsonObject("timestamp");
        Assertions.assertEquals("non-syslog", timestampJson.getString("source"));
        Assertions.assertEquals("unrecognized", timestampJson.getString("original"));
        Assertions.assertEquals(true, timestampJson.isNull("epoch"));
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EventMetadata.class).verify();
    }
}
