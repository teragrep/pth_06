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
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Objects;

final class EventMetadata {

    private final String bucket;
    private final String path;
    private final String id;

    EventMetadata(final String bucket, final String path, final String id) {
        this.bucket = bucket;
        this.path = path;
        this.id = id;
    }

    JsonObject asJSON(
            final RFC5424Frame rfc5424Frame,
            final RFC5424Timestamp rfc5424Timestamp,
            final PathExtractedTimestamp pathExtractedTimestamp,
            boolean isSyslogFormat
    ) {

        final JsonObjectBuilder rootBuilder = Json
                .createObjectBuilder()
                .add("epochMigration", true)
                .add("format", isSyslogFormat ? "rfc5424" : "non-rfc5424");

        final JsonObjectBuilder objectBuilder = Json
                .createObjectBuilder()
                .add("bucket", bucket)
                .add("path", path)
                .add("partition", id);

        rootBuilder.add("object", objectBuilder);

        final JsonObjectBuilder timestampBuilder = Json.createObjectBuilder();
        if (isSyslogFormat) {
            timestampBuilder
                    .add("original", String.valueOf(rfc5424Frame.timestamp))
                    .add("epoch", new EpochMicros(rfc5424Timestamp).asLong())
                    .add("path-extracted", new EpochMicros(pathExtractedTimestamp).asLong())
                    .add("source", "syslog");
        }
        else {
            timestampBuilder
                    .add("original", "unrecognized")
                    .addNull("epoch") // can not calculate epoch value
                    .add("path-extracted", new EpochMicros(pathExtractedTimestamp).asLong())
                    .add("source", "object-path");
        }
        rootBuilder.add("timestamp", timestampBuilder);

        return rootBuilder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final EventMetadata that = (EventMetadata) o;
        return Objects.equals(bucket, that.bucket) && Objects.equals(path, that.path) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, path, id);
    }
}
