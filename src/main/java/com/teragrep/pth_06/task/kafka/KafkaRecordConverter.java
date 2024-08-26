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
package com.teragrep.pth_06.task.kafka;

import com.teragrep.rlo_06.Fragment;
import com.teragrep.rlo_06.RFC5424Frame;
import com.teragrep.rlo_06.RFC5424Timestamp;
import com.teragrep.rlo_06.SDVector;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;

/**
 * <h1>Kafka Record Converter</h1> Class for converting Kafka data into byte[].
 *
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class KafkaRecordConverter {

    private final SDVector eventNodeSourceSource;
    private final SDVector eventNodeRelaySource;
    private final SDVector eventNodeSourceSourceModule;
    private final SDVector eventNodeRelaySourceModule;
    private final SDVector eventNodeSourceHostname;
    private final SDVector eventNodeRelayHostname;

    private final SDVector teragrepStreamName;
    private final SDVector teragrepDirectory;

    // Origin
    private final SDVector originHostname;

    // workaround cfe-06/issue/68 by subscribing the broken field
    private final RFC5424Frame rfc5424Frame;

    private final UnsafeRowWriter rowWriter = new UnsafeRowWriter(11);

    private final ByteBuffer sourceConcatenationBuffer;

    public KafkaRecordConverter() {
        this.eventNodeSourceSource = new SDVector("event_node_source@48577", "source");
        this.eventNodeRelaySource = new SDVector("event_node_relay@48577", "source");
        this.eventNodeSourceSourceModule = new SDVector("event_node_source@48577", "source_module");
        this.eventNodeRelaySourceModule = new SDVector("event_node_relay@48577", "source_module");
        this.eventNodeSourceHostname = new SDVector("event_node_source@48577", "hostname");
        this.eventNodeRelayHostname = new SDVector("event_node_relay@48577", "hostname");

        this.teragrepStreamName = new SDVector("teragrep@48577", "streamname");
        this.teragrepDirectory = new SDVector("teragrep@48577", "directory");

        // Origin
        this.originHostname = new SDVector("origin@48577", "hostname");

        this.rfc5424Frame = new RFC5424Frame();

        this.sourceConcatenationBuffer = ByteBuffer.allocateDirect(256 * 1024);
    }

    private long rfc3339ToEpoch(ZonedDateTime zonedDateTime) {
        final Instant instant = zonedDateTime.toInstant();

        final long MICROS_PER_SECOND = 1000L * 1000L;
        final long NANOS_PER_MICROS = 1000L;
        final long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);

        return Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);
    }

    public UnsafeRow convert(InputStream inputStream, String partition, long offset) {
        rfc5424Frame.load(inputStream);
        try {
            rfc5424Frame.next();
        }
        catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }

        final long epochMicros = rfc3339ToEpoch(new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime());

        // input
        final byte[] source = eventToSource();

        // origin
        final byte[] origin = eventToOrigin();

        /*
        return RowFactory.create(
                Timestamp.from(instant),                    // 0 "_time", DataTypes.TimestampType
                UTF8String.fromBytes(message).toString(),   // 1 "_raw", DataTypes.StringType
                UTF8String.fromBytes(index).toString(),     // 2 "directory", DataTypes.StringType
                UTF8String.fromBytes(sourcetype).toString(),// 3 "stream", DataTypes.StringType
                UTF8String.fromBytes(hostname).toString(),  // 4 "host", DataTypes.StringType,
                UTF8String.fromBytes(input).toString(),     // 5 "input", DataTypes.StringType
                partition,                                  // 6 "partition", DataTypes.StringType
                offset,                                     // 7 "offset", DataTypes.LongType
                UTF8String.fromBytes(origin).toString()     // 8 "origin", DataTypes.StringType
        );
        
         */

        rowWriter.reset();
        rowWriter.zeroOutNullBytes();
        rowWriter.write(0, epochMicros);
        rowWriter.write(1, UTF8String.fromBytes(rfc5424Frame.msg.toBytes()));
        rowWriter.write(2, UTF8String.fromBytes(rfc5424Frame.structuredData.getValue(teragrepDirectory).toBytes()));
        rowWriter.write(3, UTF8String.fromBytes(rfc5424Frame.structuredData.getValue(teragrepStreamName).toBytes()));
        rowWriter.write(4, UTF8String.fromBytes(rfc5424Frame.hostname.toBytes()));
        rowWriter.write(5, UTF8String.fromBytes(source));
        rowWriter.write(6, UTF8String.fromBytes(partition.getBytes(StandardCharsets.UTF_8)));
        rowWriter.write(7, offset);
        rowWriter.write(8, UTF8String.fromBytes(origin));

        return rowWriter.getRow();
    }

    private byte[] eventToOrigin() {
        byte[] origin;
        Fragment originFragment = rfc5424Frame.structuredData.getValue(originHostname);
        if (!originFragment.isStub) {
            origin = originFragment.toBytes();
        }
        else {
            origin = new byte[] {};
        }
        return origin;
    }

    private byte[] eventToSource() {
        //input is produced from SD element event_node_source@48577 by
        // concatenating "source_module:hostname:source". in case
        //if event_node_source@48577 is not available use event_node_relay@48577.
        //If neither are present, use null value.

        sourceConcatenationBuffer.clear();

        Fragment sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSourceModule);
        if (sourceModuleFragment.isStub) {
            sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySourceModule);
        }

        byte[] source_module;
        if (!sourceModuleFragment.isStub) {
            source_module = sourceModuleFragment.toBytes();
        }
        else {
            source_module = new byte[] {};
        }

        Fragment sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceHostname);
        if (sourceHostnameFragment.isStub) {
            sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeRelayHostname);
        }

        byte[] source_hostname;
        if (!sourceHostnameFragment.isStub) {
            source_hostname = sourceHostnameFragment.toBytes();
        }
        else {
            source_hostname = new byte[] {};
        }

        Fragment sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSource);
        if (sourceHostnameFragment.isStub) {
            sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySource);
        }

        byte[] source_source;
        if (!sourceSourceFragment.isStub) {
            source_source = sourceSourceFragment.toBytes();
        }
        else {
            source_source = new byte[] {};
        }

        // source_module:hostname:source"
        sourceConcatenationBuffer.put(source_module);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_hostname);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_source);

        sourceConcatenationBuffer.flip();
        byte[] input = new byte[sourceConcatenationBuffer.remaining()];
        sourceConcatenationBuffer.get(input);

        return input;
    }
}
