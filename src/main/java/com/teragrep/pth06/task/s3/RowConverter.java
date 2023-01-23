package com.teragrep.pth06.task.s3;

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

import com.amazonaws.services.s3.AmazonS3;
import com.teragrep.rad_01.AuditPlugin;
import com.teragrep.rlo_06.ParserResultset;
import com.teragrep.rlo_06.ResultsetAsByteBuffer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * <h1>Row Converter</h1>
 *
 * Converts S3 object rows.
 */
public final class RowConverter {
    private final Logger LOGGER = LoggerFactory.getLogger(RowConverter.class);

    // audit plugin
    private final AuditPlugin auditPlugin;

    // offset within the object
    private long currentOffset;

	private final String bucket;
    private final String path;

    // for rows
    private final UTF8String id;
    private final UTF8String directory;
    private final UTF8String stream;
    private final UTF8String host;

    // subscribed fields as bytebuffer
    private final ByteBuffer eventNodeSourceBB;
    private final ByteBuffer eventNodeRelayBB;
    private final ByteBuffer sourceModuleBB;
    private final ByteBuffer hostnameBB;
    private final ByteBuffer sourceBB;
    private final ByteBuffer sourceStringBB;

    // origin
    private final ByteBuffer originSDIDBB;
    private final ByteBuffer originSDEhostnameBB;
    private final ByteBuffer originStringBB;


    // Currently handled log event from S3-file
    private ParserResultset currentLogEvent = null;
    private ResultsetAsByteBuffer resultsetAsByteBuffer = new ResultsetAsByteBuffer(null);

    private final UnsafeRowWriter rowWriter = new UnsafeRowWriter(11);

    // S3 client is file specific
    private final RFC5424FileReader rfc5424FileReader;

    public RowConverter(
                        AuditPlugin auditPlugin,
                        AmazonS3 s3client,
						String id,
                        String bucket,
                        String path,
                        String directory,
                        String stream,
                        String host,
                        boolean skipNonRFC5424Files) {
        this.auditPlugin = auditPlugin;

	    this.bucket = bucket;
        this.path = path;

        this.id = UTF8String.fromString(id);
        this.directory = UTF8String.fromString(directory.toLowerCase());
        this.stream = UTF8String.fromString(stream.toLowerCase());
        this.host = UTF8String.fromString(host.toLowerCase());

        if(LOGGER.isDebugEnabled())
            LOGGER.debug("RowConverter> created with partition:" + this.bucket +
                " path: " + this.path );


        rfc5424FileReader = new RFC5424FileReader(s3client, skipNonRFC5424Files);

        if (LOGGER.isDebugEnabled())
            LOGGER.info("Initialized s3client:" + s3client);

        // initial status
        this.currentOffset = 0L;

        // match string initializers, here to make them finals as optimized for instantiation
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

    }

    public void open() throws IOException {
        // Open s3-file
        rfc5424FileReader.open(this.bucket, this.path);
    }

    // read zip until it ends
    public boolean next() throws IOException {
        boolean rv = true;        
        if(LOGGER.isDebugEnabled())        
            LOGGER.debug("RowConverter.next> currentOffset: " +
                currentOffset);
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("<" + currentOffset + ">  Read event from S3-file: " + bucket + "/" + path);

        if (rfc5424FileReader == null) {
            throw new IOException("IOError when accessing s3-File:" + bucket + "/" + path);
        }
        if (rfc5424FileReader.hasNext()) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("<" + this.currentOffset + "> Read event: " + currentLogEvent);
            currentLogEvent = rfc5424FileReader.next();
            this.currentOffset++;
        } else {
            rv = false;
        }
        return rv;
    }

    public static long rfc3339ToEpoch(ByteBuffer rfc3339Timestamp) {
        final Instant instant = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(
                StandardCharsets.ISO_8859_1.decode(
                        rfc3339Timestamp
                ), Instant::from);

        final long MICROS_PER_SECOND = 1000L * 1000L;
        final long NANOS_PER_MICROS = 1000L;
        final long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);

        return Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);
    }

    public InternalRow get() {
        //System.out.println("RowConverter.get>");
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("RowConverter.get> Partition ("+this.id+"):" + bucket + "/" + path+ " Get("+currentOffset +")");

        if (currentLogEvent != null) {
            resultsetAsByteBuffer.setResultset(currentLogEvent);
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("Parsersyslog event:"+currentLogEvent);

            final long epochMicros =  rfc3339ToEpoch(resultsetAsByteBuffer.getTIMESTAMP());

            // input
            final byte[] input = eventToInput();

            // origin
            final byte[] origin = eventToOrigin();

            // message
            final ByteBuffer messageBB = resultsetAsByteBuffer.getMSG();
            final byte[] message = new byte[messageBB.remaining()];
            messageBB.get(message);

            if(LOGGER.isDebugEnabled()){
                LOGGER.trace("PARSED  epochMicros: "+epochMicros+ "  message: "+message);
           }

            rowWriter.reset();
            rowWriter.zeroOutNullBytes();
            rowWriter.write(0, epochMicros);
            rowWriter.write(1, UTF8String.fromBytes(message));
            rowWriter.write(2, this.directory);
            rowWriter.write(3, this.stream);
            rowWriter.write(4, this.host);
            rowWriter.write(5, UTF8String.fromBytes(input));
            rowWriter.write(6, this.id);
            rowWriter.write(7, currentOffset);
            rowWriter.write(8, UTF8String.fromBytes(origin));


            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Get Event,  row=written");

            auditPlugin.audit(
                    epochMicros,
                    message,
                    this.directory.getBytes(),
                    this.stream.getBytes(),
                    this.host.getBytes(),
                    input,
                    this.id.toString(),
                    currentOffset
                    );
            return rowWriter.getRow();

        } else {
            LOGGER.warn("Missing actual LogEvent");
            return null;
        }
    }

    private Timestamp eventTimeToTimestamp(ByteBuffer timestampStr) {
        Timestamp timestamp = null;
        if (timestampStr != null) {

            timestamp = Timestamp.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(StandardCharsets.ISO_8859_1.decode(timestampStr), Instant::from));

        } else {
            LOGGER.error("Null timestamp");
        }
        return timestamp;
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

    public void close() throws IOException {
        this.rfc5424FileReader.close();
    }
}
