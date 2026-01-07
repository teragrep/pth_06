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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.teragrep.rlo_06.ParseException;
import com.teragrep.rlo_06.RFC5424Frame;
import com.teragrep.rlo_06.RFC5424Timestamp;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public final class EpochMigrationRowConverter implements RowConverter {

    private final Logger LOGGER = LoggerFactory.getLogger(EpochMigrationRowConverter.class);

    // offset within the object
    private long currentOffset;

    private final String bucket;
    private final String path;

    // for rows
    private final UTF8String id;
    private final UTF8String directory;
    private final UTF8String stream;
    private final UTF8String host;

    private final EpochMigrationRawEnvelope jsonEnvelope;
    // Currently handled log event from S3-file
    private final UnsafeRowWriter rowWriter;

    private final RFC5424Frame rfc5424Frame;

    private final AmazonS3 s3client;

    private InputStream inputStream = null;

    private boolean isSyslogFormat;

    public EpochMigrationRowConverter(
            final AmazonS3 s3client,
            final String id,
            final String bucket,
            final String path,
            final String directory,
            final String stream,
            final String host
    ) {
        this(s3client, id, bucket, path, directory, stream, host, new EpochMigrationRawEnvelope(bucket, path, id));
    }

    public EpochMigrationRowConverter(
            final AmazonS3 s3client,
            final String id,
            final String bucket,
            final String path,
            final String directory,
            final String stream,
            final String host,
            final EpochMigrationRawEnvelope jsonEnvelope
    ) {
        this.bucket = bucket;
        this.path = path;

        this.id = UTF8String.fromString(id);
        this.directory = UTF8String.fromString(directory.toLowerCase());
        this.stream = UTF8String.fromString(stream.toLowerCase());
        this.host = UTF8String.fromString(host.toLowerCase());
        this.jsonEnvelope = jsonEnvelope;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("RowConverter created with partition <[{}]> path <[{}]>", bucket, path);
        }

        this.rowWriter = new UnsafeRowWriter(11);

        this.s3client = s3client;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("Initialized s3client <{}>", s3client);
        }

        // initial status
        this.currentOffset = 0L;
        this.isSyslogFormat = false;

        this.rfc5424Frame = new RFC5424Frame(true);

    }

    @Override
    public void open() throws IOException {
        // Open s3-file
        final String logName = bucket + "/" + path;
        final S3Object s3object;
        try {
            LOGGER.debug("Attempting to open file <[{}]>", logName);
            s3object = s3client.getObject(bucket, path);

            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug(
                                "Open S3 stream bucket <[{}]> keyname <[{}]> Metadata length <{}>", bucket, path,
                                s3object.getObjectMetadata().getContentLength()
                        );
            }
            this.inputStream = new BufferedInputStream(s3object.getObjectContent(), 8 * 1024 * 1024);
            GZIPInputStream gz = new GZIPInputStream(inputStream);
            rfc5424Frame.load(gz);

            // Set up result
            LOGGER.trace("S3FileHandler.open() Initialized result set with element lists");

            LOGGER.info("S3FileHandler.open() Initialized parser for <[{}]>", logName);
        }
        catch (final AmazonServiceException amazonServiceException) {

            if (403 == amazonServiceException.getStatusCode()) {
                LOGGER.error("Skipping file <[{}]> due to errorCode <{}>", logName, 403);
            }
            else {
                throw amazonServiceException;
            }
        }
    }

    // read zip until first event
    @Override
    public boolean next() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("RowConverter.next() called on offset <{}> ", currentOffset);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("<{}>  Read event from S3-file: <[{}]>/<[{}]>", currentOffset, bucket, path);
        }

        boolean rv;
        // first event read
        if (currentOffset > 0) {
            rv = false;
        }
        else {
            try {
                rv = rfc5424Frame.next();
                if (rv) {
                    this.currentOffset++;
                    this.isSyslogFormat = true;

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("<{}> Read first event: <[{}]>", currentOffset, rfc5424Frame);
                    }
                }
            }
            // non syslog format, allow to continue once
            catch (final ParseException parseException) {
                LOGGER.error("ParseException at object: <[{}]>/<[{}]>", bucket, path);
                currentOffset++;
                this.isSyslogFormat = false;
                rv = true;
            }
        }
        return rv;
    }

    @Override
    public InternalRow get() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "EpochMigrationRowConverter.get() partition=<[{}]>, bucket=<[{}]> path=<[{}]> offset=<{}>",
                            id, bucket, path, currentOffset
                    );
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Parser syslog event <[{}]>", rfc5424Frame.toString());
        }

        final EpochMicros epochMicros = new EpochMicros(new RFC5424Timestamp(rfc5424Frame.timestamp));

        final UTF8String jsonEnvelopeString = jsonEnvelope
                .asJSONFrom(rfc5424Frame, epochMicros, currentOffset, isSyslogFormat);

        rowWriter.reset();
        rowWriter.zeroOutNullBytes();
        if (isSyslogFormat) {
            rowWriter.write(0, epochMicros.asLong());
            rowWriter.write(1, jsonEnvelopeString);
            rowWriter.write(2, this.directory);
            rowWriter.write(3, this.stream);
            rowWriter.write(4, this.host);
            rowWriter.write(5, new EventToSource().asUTF8StringFrom(rfc5424Frame));
            rowWriter.write(6, this.id);
            rowWriter.write(7, currentOffset);
            rowWriter.write(8, new EventToOrigin().asUTF8StringFrom(rfc5424Frame));
        }
        else {
            rowWriter.write(0, 0L);
            rowWriter.write(1, jsonEnvelopeString);
            rowWriter.write(2, this.directory);
            rowWriter.write(3, this.stream);
            rowWriter.write(4, this.host);
            rowWriter.write(5, "unknown-source".getBytes(StandardCharsets.UTF_8));
            rowWriter.write(6, this.id);
            rowWriter.write(7, currentOffset);
            rowWriter.write(8, "unknown-origin".getBytes(StandardCharsets.UTF_8));
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Get Event,  row=written");
        }

        return rowWriter.getRow();
    }

    @Override
    public void close() throws IOException {
        final String logName = bucket + "/" + path;
        LOGGER.info("S3FileHandler.close() on log <{}> on offset <{}>", logName, currentOffset);
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
