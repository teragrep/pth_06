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

package com.teragrep.pth06.task.kafka;

import com.teragrep.rlo_06.ParserResultset;
import com.teragrep.rlo_06.ResultsetAsByteBuffer;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.teragrep.pth06.task.s3.RowConverter.rfc3339ToEpoch;

/**
 * <h1>Kafka Record Converter</h1>
 *
 * Class for converting Kafka data into byte[].
 *
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class KafkaRecordConverter {
    // subscribed fields as bytebuffer
    private final ByteBuffer eventNodeSourceBB;
    private final ByteBuffer eventNodeRelayBB;
    private final ByteBuffer sourceModuleBB;
    private final ByteBuffer hostnameBB;
    private final ByteBuffer sourceBB;

    private final ByteBuffer teragrepBB;
    private final ByteBuffer indexBB;
    private final ByteBuffer sourcetypeBB;

    private final ByteBuffer sourceStringBB;

    // origin
    private final ByteBuffer originSDIDBB;
    private final ByteBuffer originSDEhostnameBB;
    private final ByteBuffer originStringBB;

    private final ResultsetAsByteBuffer resultsetAsByteBuffer;

    private final UnsafeRowWriter rowWriter = new UnsafeRowWriter(11);

    public KafkaRecordConverter() {
        // match string initializers, used for querying the resultset
        byte[] ens = "event_node_source@48577".getBytes(StandardCharsets.US_ASCII);
        this.eventNodeSourceBB = ByteBuffer.allocateDirect(ens.length);
        this.eventNodeSourceBB.put(ens, 0, ens.length);
        this.eventNodeSourceBB.flip();

        byte[] enr = "event_node_relay@48577".getBytes(StandardCharsets.US_ASCII);
        this.eventNodeRelayBB = ByteBuffer.allocateDirect(enr.length);
        this.eventNodeRelayBB.put(enr, 0, enr.length);
        this.eventNodeRelayBB.flip();

        byte[] sm = "source_module".getBytes(StandardCharsets.US_ASCII);
        this.sourceModuleBB = ByteBuffer.allocateDirect(sm.length);
        this.sourceModuleBB.put(sm, 0, sm.length);
        this.sourceModuleBB.flip();

        byte[] hn = "hostname".getBytes(StandardCharsets.US_ASCII);
        this.hostnameBB = ByteBuffer.allocateDirect(hn.length);
        this.hostnameBB.put(hn, 0, hn.length);
        this.hostnameBB.flip();

        byte[] source = "source".getBytes(StandardCharsets.US_ASCII);
        this.sourceBB = ByteBuffer.allocateDirect(source.length);
        this.sourceBB.put(source, 0, source.length);
        this.sourceBB.flip();

        // teragrep
        byte[] teragrep = "teragrep@48577".getBytes(StandardCharsets.US_ASCII);
        this.teragrepBB = ByteBuffer.allocateDirect(teragrep.length);
        this.teragrepBB.put(teragrep, 0, teragrep.length);
        this.teragrepBB.flip();

        byte[] index = "directory".getBytes(StandardCharsets.US_ASCII);
        this.indexBB = ByteBuffer.allocateDirect(index.length);
        this.indexBB.put(index, 0, index.length);
        this.indexBB.flip();

        byte[] sourcetype = "streamname".getBytes(StandardCharsets.US_ASCII);
        this.sourcetypeBB = ByteBuffer.allocateDirect(sourcetype.length);
        this.sourcetypeBB.put(sourcetype, 0, sourcetype.length);
        this.sourcetypeBB.flip();

        this.sourceStringBB = ByteBuffer.allocateDirect(8*1024 + 1 + 8*1024 + 1 + 8*1024);

        // origin
        byte[] originSDID = "origin@48577".getBytes(StandardCharsets.US_ASCII);
        this.originSDIDBB = ByteBuffer.allocateDirect(originSDID.length);
        this.originSDIDBB.put(originSDID, 0, originSDID.length);
        this.originSDIDBB.flip();

        byte[] originSDEhostname = "hostname".getBytes(StandardCharsets.US_ASCII);
        this.originSDEhostnameBB = ByteBuffer.allocateDirect(originSDEhostname.length);
        this.originSDEhostnameBB.put(originSDEhostname, 0, originSDEhostname.length);
        this.originSDEhostnameBB.flip();

        this.originStringBB = ByteBuffer.allocateDirect(8*1024);

        // resultset processor
        this.resultsetAsByteBuffer = new ResultsetAsByteBuffer(null);
    }



    public UnsafeRow convert(ParserResultset resultset, String partition, long offset) {
        resultsetAsByteBuffer.setResultset(resultset);

        final long epochMicros =  rfc3339ToEpoch(resultsetAsByteBuffer.getTIMESTAMP());


        // input
        final byte[] input = eventToInput();

        // origin
        final byte[] origin = eventToOrigin();

        // message
        final ByteBuffer messageBB = resultsetAsByteBuffer.getMSG();
        final byte[] message = new byte[messageBB.remaining()];
        messageBB.get(message);

        // message
        final ByteBuffer hostnameBB = resultsetAsByteBuffer.getHOSTNAME();
        final byte[] hostname = new byte[hostnameBB.remaining()];
        hostnameBB.get(hostname);

        // index
        ByteBuffer indexResBB = resultsetAsByteBuffer.getSdValue(teragrepBB, indexBB);
        final byte[] index = new byte[indexResBB.remaining()];
        indexResBB.get(index);

        // sourcetype
        ByteBuffer sourcetypeResBB = resultsetAsByteBuffer.getSdValue(teragrepBB, sourcetypeBB);
        final byte[] sourcetype = new byte[sourcetypeResBB.remaining()];
        sourcetypeResBB.get(sourcetype);

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
        rowWriter.write(1, UTF8String.fromBytes(message));
        rowWriter.write(2,  UTF8String.fromBytes(index));
        rowWriter.write(3, UTF8String.fromBytes(sourcetype));
        rowWriter.write(4, UTF8String.fromBytes(hostname));
        rowWriter.write(5, UTF8String.fromBytes(input));
        rowWriter.write(6, UTF8String.fromBytes(partition.getBytes(StandardCharsets.UTF_8)));
        rowWriter.write(7, offset);
        rowWriter.write(8, UTF8String.fromBytes(origin));

        return rowWriter.getRow();
    }

    private byte[] eventToOrigin() {
        ByteBuffer origin_element = resultsetAsByteBuffer.getSdValue(originSDIDBB, originSDEhostnameBB);
        originStringBB.clear();
        if (origin_element != null) {
            originStringBB.put(origin_element);
        }
        originStringBB.flip();
        byte[] input = new byte[originStringBB.remaining()];
        originStringBB.get(input);
        return input;
    }

    private byte[] eventToInput() {
        //input is produced from SD element event_node_source@48577 by
        // concatenating "source_module:hostname:source". in case
        //if event_node_source@48577 is not available use event_node_relay@48577.
        //If neither are present, use null value.

        ByteBuffer source_module = resultsetAsByteBuffer.getSdValue(eventNodeSourceBB, sourceModuleBB);
        if(source_module == null || !source_module.hasRemaining()){
            source_module = resultsetAsByteBuffer.getSdValue(eventNodeRelayBB, sourceModuleBB);
        }
        ByteBuffer hostname = resultsetAsByteBuffer.getSdValue(eventNodeSourceBB, hostnameBB);
        if(hostname == null || !hostname.hasRemaining()){
            hostname = resultsetAsByteBuffer.getSdValue(eventNodeRelayBB, hostnameBB);
        }
        ByteBuffer source = resultsetAsByteBuffer.getSdValue(eventNodeSourceBB, sourceBB);
        if(source == null || !source.hasRemaining()){
            source = resultsetAsByteBuffer.getSdValue(eventNodeRelayBB, sourceBB);
        }

        // sm:hn:s
        sourceStringBB.clear();
        // source_module:hostname:source"
        if(source_module != null) {
            sourceStringBB.put(source_module);
        }
        sourceStringBB.put((byte) ':');
        if (hostname != null) {
            sourceStringBB.put(hostname);
        }
        sourceStringBB.put((byte)':');
        if (source != null) {
            sourceStringBB.put(source);
        }

        sourceStringBB.flip();
        byte[] input = new byte[sourceStringBB.remaining()];
        sourceStringBB.get(input);

        return input;
    }
}
