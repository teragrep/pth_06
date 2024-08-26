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
package com.teragrep.pth_06.task;

import com.google.gson.Gson;
import com.teragrep.pth_06.ArchiveS3ObjectMetadata;
import com.teragrep.rad_01.AuditPlugin;
import com.teragrep.rad_01.AuditPluginFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedList;

public class MetadataMicroBatchInputPartitionReader implements PartitionReader<InternalRow> {

    final Logger LOGGER = LoggerFactory.getLogger(MetadataMicroBatchInputPartitionReader.class);

    private final AuditPlugin auditPlugin;
    private UnsafeRowWriter rowWriter;

    private final LinkedList<ArchiveS3ObjectMetadata> taskObjectList;

    private long currentOffset;

    public MetadataMicroBatchInputPartitionReader(
            LinkedList<ArchiveS3ObjectMetadata> taskObjectList,
            String TeragrepAuditQuery,
            String TeragrepAuditReason,
            String TeragrepAuditUser,
            String TeragrepAuditPluginClassName
    ) {
        this.taskObjectList = taskObjectList;

        AuditPluginFactory auditPluginFactory = new AuditPluginFactory(TeragrepAuditPluginClassName);

        try {
            this.auditPlugin = auditPluginFactory.getAuditPlugin();
            this.auditPlugin.setQuery(TeragrepAuditQuery);
            this.auditPlugin.setReason(TeragrepAuditReason);
            this.auditPlugin.setUser(TeragrepAuditUser);

        }
        catch (
                ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException
                | NoSuchMethodException e
        ) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        this.rowWriter = new UnsafeRowWriter(11);
        this.currentOffset = 0L;

    }

    @Override
    public boolean next() throws IOException {
        if (taskObjectList.isEmpty()) {
            return false;
        }
        else {
            rowWriter.reset();
            rowWriter.zeroOutNullBytes();
            ArchiveS3ObjectMetadata taskObject = taskObjectList.removeFirst();

            // Use metadata java object to easily form a json representation of metadata
            final String rawColumn = new Gson()
                    .toJson(new Metadata(taskObject.uncompressedSize, taskObject.compressedSize));
            // use logtimeEpoch as _time
            rowWriter
                    .write(0, rfc3339ToEpoch(Instant.ofEpochSecond(taskObject.logtimeEpoch).atZone(ZoneId.systemDefault())));
            rowWriter.write(1, UTF8String.fromString(rawColumn));
            rowWriter.write(2, UTF8String.fromString(taskObject.directory));
            rowWriter.write(3, UTF8String.fromString(taskObject.stream));
            rowWriter.write(4, UTF8String.fromString(taskObject.host));
            rowWriter.write(5, UTF8String.fromString(""));
            rowWriter.write(6, UTF8String.fromString(taskObject.id));
            rowWriter.write(7, currentOffset++);
            rowWriter.write(8, UTF8String.fromString(""));
            return true;
        }
    }

    @Override
    public InternalRow get() {
        /*auditPlugin.audit(
                epochMicros,
                rfc5424Frame.msg.toBytes(),
                this.directory.getBytes(),
                this.stream.getBytes(),
                this.host.getBytes(),
                source,
                this.id.toString(),
                currentOffset
        );*/
        return rowWriter.getRow();
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    static long rfc3339ToEpoch(ZonedDateTime zonedDateTime) {
        final Instant instant = zonedDateTime.toInstant();

        final long MICROS_PER_SECOND = 1000L * 1000L;
        final long NANOS_PER_MICROS = 1000L;
        final long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);

        return Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);
    }
}
