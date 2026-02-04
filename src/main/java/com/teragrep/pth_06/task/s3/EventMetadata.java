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

    JsonObject toJSONWithSyslogTimestamp(final RFC5424Frame rfc5424Frame) {
        final RFC5424Timestamp rfc5424Timestamp = new RFC5424Timestamp(rfc5424Frame.timestamp);
        final JsonObjectBuilder rootBuilder = Json
                .createObjectBuilder()
                .add("epochMigration", true)
                .add("format", "rfc5424");

        final JsonObjectBuilder objectBuilder = Json
                .createObjectBuilder()
                .add("bucket", bucket)
                .add("path", path)
                .add("partition", id);

        rootBuilder.add("object", objectBuilder);

        final JsonObjectBuilder timestampBuilder = Json.createObjectBuilder();

        final EpochMicros epochMicros = new EpochMicros(rfc5424Timestamp);
        final PathExtractedTimestamp pathExtractedTimestamp = new PathExtractedTimestamp(path);
        timestampBuilder
                .add("rfc5242timestamp", String.valueOf(rfc5424Frame.timestamp))
                .add("epoch", epochMicros.asLong())
                .add("path-extracted", pathExtractedTimestamp.toZonedDateTime().toString())
                .add("path-extracted-precision", pathExtractedTimestamp.hasHourlyData() ? "hourly" : "daily")
                .add("source", epochMicros.source());

        rootBuilder.add("timestamp", timestampBuilder);

        return rootBuilder.build();
    }

    JsonObject toJSONWithPathExtractedTimestamp() {
        final JsonObjectBuilder rootBuilder = Json
                .createObjectBuilder()
                .add("epochMigration", true)
                .add("format", "non-rfc5424");

        final JsonObjectBuilder objectBuilder = Json
                .createObjectBuilder()
                .add("bucket", bucket)
                .add("path", path)
                .add("partition", id);

        rootBuilder.add("object", objectBuilder);

        final JsonObjectBuilder timestampBuilder = Json.createObjectBuilder();
        final PathExtractedTimestamp pathExtractedTimestamp = new PathExtractedTimestamp(path);
        final EpochMicros epochMicros = new EpochMicros(pathExtractedTimestamp);
        timestampBuilder
                .add("path-extracted", pathExtractedTimestamp.toZonedDateTime().toString())
                .add("path-extracted-precision", pathExtractedTimestamp.hasHourlyData() ? "hourly" : "daily")
                .add("source", epochMicros.source());
        rootBuilder.add("timestamp", timestampBuilder);

        return rootBuilder.build();
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final EventMetadata that = (EventMetadata) object;
        return Objects.equals(bucket, that.bucket) && Objects.equals(path, that.path) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, path, id);
    }
}
