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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.teragrep.rad_01.AuditPlugin;
import com.teragrep.rlo_06.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.zip.GZIPInputStream;

/**
 * <h1>Row Converter</h1> Converts S3 object rows.
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

    // Currently handled log event from S3-file
    private final UnsafeRowWriter rowWriter;

    private final ByteBuffer sourceConcatenationBuffer;

    private final RFC5424Frame rfc5424Frame;

    private final AmazonS3 s3client;

    private final SDVector eventNodeSourceSource;
    private final SDVector eventNodeRelaySource;
    private final SDVector eventNodeSourceSourceModule;
    private final SDVector eventNodeRelaySourceModule;
    private final SDVector eventNodeSourceHostname;
    private final SDVector eventNodeRelayHostname;
    private final SDVector originHostname;

    final boolean skipNonRFC5424Files;

    private InputStream inputStream = null;

    public RowConverter(
            AuditPlugin auditPlugin,
            AmazonS3 s3client,
            String id,
            String bucket,
            String path,
            String directory,
            String stream,
            String host,
            boolean skipNonRFC5424Files
    ) {
        this.auditPlugin = auditPlugin;

        this.bucket = bucket;
        this.path = path;

        this.id = UTF8String.fromString(id);
        this.directory = UTF8String.fromString(directory.toLowerCase());
        this.stream = UTF8String.fromString(stream.toLowerCase());
        this.host = UTF8String.fromString(host.toLowerCase());

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("RowConverter> created with partition:" + this.bucket + " path: " + this.path);

        this.rowWriter = new UnsafeRowWriter(11);

        this.s3client = s3client;

        this.skipNonRFC5424Files = skipNonRFC5424Files;

        if (LOGGER.isDebugEnabled())
            LOGGER.info("Initialized s3client:" + s3client);

        // initial status
        this.currentOffset = 0L;

        this.sourceConcatenationBuffer = ByteBuffer.allocateDirect(256 * 1024);

        this.rfc5424Frame = new RFC5424Frame(true);

        this.eventNodeSourceSource = new SDVector("event_node_source@48577", "source");
        this.eventNodeRelaySource = new SDVector("event_node_relay@48577", "source");
        this.eventNodeSourceSourceModule = new SDVector("event_node_source@48577", "source_module");
        this.eventNodeRelaySourceModule = new SDVector("event_node_relay@48577", "source_module");
        this.eventNodeSourceHostname = new SDVector("event_node_source@48577", "hostname");
        this.eventNodeRelayHostname = new SDVector("event_node_relay@48577", "hostname");
        this.originHostname = new SDVector("origin@48577", "hostname");

    }

    public void open() throws IOException {
        // Open s3-file

        String bucketName = this.bucket;
        String keyName = this.path;

        String logName = bucketName + "/" + keyName;
        S3Object s3object;
        try {
            LOGGER.debug("Attempting to open file: " + logName);
            s3object = s3client.getObject(bucketName, keyName);

            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug(
                                "Open S3 stream bucket:" + bucketName + " keyname=" + keyName + " Metadata length:"
                                        + s3object.getObjectMetadata().getContentLength()
                        );
            }
            this.inputStream = new BufferedInputStream(s3object.getObjectContent(), 8 * 1024 * 1024);
            GZIPInputStream gz = new GZIPInputStream(inputStream);
            rfc5424Frame.load(gz);

            // Set up result
            LOGGER.trace("S3FileHandler.open() Initialized result set with element lists");

            LOGGER.info("S3FileHandler.open() Initialized parser for: " + logName);
        }
        catch (AmazonServiceException amazonServiceException) {

            if (403 == amazonServiceException.getStatusCode()) {
                LOGGER
                        .error(
                                "Skipping file " + logName + " due to errorCode:"
                                        + amazonServiceException.getStatusCode()
                        );
            }
            else {
                throw amazonServiceException;
            }
        }
    }

    // read zip until it ends
    public boolean next() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("RowConverter.next> currentOffset: " + currentOffset);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("<{}>  Read event from S3-file: <[{}]>/<[{}]>", currentOffset, bucket, path);
        }

        boolean rv;
        try {
            rv = rfc5424Frame.next();
            if (rv) {
                this.currentOffset++;

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("<{}> Read event: <[{}]>", currentOffset, rfc5424Frame);
                }
            }
        }
        catch (ParseException parseException) {
            LOGGER.error("ParseException at object: <[{}]>/<[{}]>", bucket, path);
            if (skipNonRFC5424Files) {
                rv = false;
            }
            else {
                throw parseException;
            }
        }
        return rv;
    }

    static long rfc3339ToEpoch(ZonedDateTime zonedDateTime) {
        final Instant instant = zonedDateTime.toInstant();

        final long MICROS_PER_SECOND = 1000L * 1000L;
        final long NANOS_PER_MICROS = 1000L;
        final long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);

        return Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);
    }

    public InternalRow get() {
        //System.out.println("RowConverter.get>");
        if (LOGGER.isDebugEnabled())
            LOGGER
                    .debug(
                            "RowConverter.get> Partition (" + this.id + "):" + bucket + "/" + path + " Get("
                                    + currentOffset + ")"
                    );

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Parsersyslog event:" + rfc5424Frame.toString());
        }

        final long epochMicros = rfc3339ToEpoch(new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime());

        // input
        final byte[] source = eventToSource();

        // origin
        final byte[] origin = eventToOrigin();

        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .trace(
                            "PARSED  epochMicros: " + epochMicros + "  message: "
                                    + new String(rfc5424Frame.msg.toBytes(), StandardCharsets.UTF_8)
                    );
        }

        rowWriter.reset();
        rowWriter.zeroOutNullBytes();
        rowWriter.write(0, epochMicros);
        rowWriter.write(1, UTF8String.fromBytes(rfc5424Frame.msg.toBytes()));
        rowWriter.write(2, this.directory);
        rowWriter.write(3, this.stream);
        rowWriter.write(4, this.host);
        rowWriter.write(5, UTF8String.fromBytes(source));
        rowWriter.write(6, this.id);
        rowWriter.write(7, currentOffset);
        rowWriter.write(8, UTF8String.fromBytes(origin));

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Get Event,  row=written");

        auditPlugin
                .audit(
                        epochMicros, rfc5424Frame.msg.toBytes(), this.directory.getBytes(), this.stream.getBytes(),
                        this.host.getBytes(), source, this.id.toString(), currentOffset
                );
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

    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
