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
package com.teragrep.pth_06;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.SDElement;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.teragrep.pth_06.planner.MockDBRow;
import com.teragrep.pth_06.planner.RecordableMockDBRow;
import com.teragrep.pth_06.planner.TestDataSource;
import com.teragrep.pth_06.task.s3.Pth06S3Client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public final class MockS3DataProvider {

    private final TestDataSource testDataSource;
    private final MockS3Configuration mockS3Configuration;

    public MockS3DataProvider(final TestDataSource testDataSource, final MockS3Configuration mockS3Configuration) {
        this.testDataSource = testDataSource;
        this.mockS3Configuration = mockS3Configuration;
    }

    public long preloadS3Data() throws IOException {
        long rows = 0L;
        final AmazonS3 amazonS3 = new Pth06S3Client(
                mockS3Configuration.s3endpoint(),
                mockS3Configuration.s3identity(),
                mockS3Configuration.s3credential()
        ).build();

        final PriorityQueue<MockDBRow> mockDBRows = testDataSource.asPriorityQueue();

        for (final MockDBRow row : mockDBRows) {
            // id, directory, stream, host, logtag, logdate, bucket, path, logtime, filesize
            final String host = row.host();
            final String logtag = row.logtag();
            final String bucket = row.bucket();
            final String path = row.path();
            final long logtime = row.logtime();

            final String recordAsJson = new RecordableMockDBRow(row).asRecord().formatJSON();

            // <46>1 2010-01-01T12:34:56.123456+02:00 hostname.domain.tld pstats - -
            SyslogMessage syslog = new SyslogMessage();
            syslog = syslog
                    .withFacility(Facility.USER)
                    .withSeverity(Severity.WARNING)
                    .withTimestamp(logtime)
                    .withHostname(host)
                    .withAppName(logtag)
                    .withMsg(recordAsJson);

            // [event_id@48577 hostname="hostname.domain.tld" uuid="" unixtime="" id_source="source"]

            final SDElement event_id_48577 = new SDElement("event_id@48577")
                    .addSDParam("hostname", host)
                    .addSDParam("uuid", UUID.randomUUID().toString())
                    .addSDParam("source", "source")
                    .addSDParam("unixtime", Long.toString(System.currentTimeMillis()));

            syslog = syslog.withSDElement(event_id_48577);

            // [event_format@48577 original_format="rfc5424"]

            final SDElement event_format_48577 = new SDElement("event_id@48577")
                    .addSDParam("original_format", "rfc5424");

            syslog = syslog.withSDElement(event_format_48577);

            // [event_node_relay@48577 hostname="relay.domain.tld" source="hostname.domain.tld" source_module="imudp"]

            final SDElement event_node_relay_48577 = new SDElement("event_node_relay@48577")
                    .addSDParam("hostname", "relay.domain.tld")
                    .addSDParam("source", host)
                    .addSDParam("source_module", "imudp");

            syslog = syslog.withSDElement(event_node_relay_48577);

            // [event_version@48577 major="2" minor="2" hostname="relay.domain.tld" version_source="relay"]

            final SDElement event_version_48577 = new SDElement("event_version@48577")
                    .addSDParam("major", "2")
                    .addSDParam("minor", "2")
                    .addSDParam("hostname", "relay.domain.tld")
                    .addSDParam("version_source", "relay");

            syslog = syslog.withSDElement(event_version_48577);

            // [event_node_router@48577 source="relay.domain.tld" source_module="imrelp" hostname="router.domain.tld"]

            final SDElement event_node_router_48577 = new SDElement("event_node_router@48577")
                    .addSDParam("source", "relay.domain.tld")
                    .addSDParam("source_module", "imrelp")
                    .addSDParam("hostname", "router.domain.tld");

            syslog = syslog.withSDElement(event_node_router_48577);

            // [origin@48577 hostname="original.hostname.domain.tld"]

            final SDElement origin_48577 = new SDElement("origin@48577")
                    .addSDParam("hostname", "original.hostname.domain.tld");
            syslog = syslog.withSDElement(origin_48577);

            // check if this bucket exists
            boolean bucketExists = false;
            for (Bucket existingBucket : amazonS3.listBuckets()) {
                if (existingBucket.getName().equals(bucket)) {
                    bucketExists = true;
                    break;
                }
            }
            if (!bucketExists) {
                amazonS3.createBucket(bucket);
            }

            // compress the message
            final String syslogMessage = syslog.toRfc5424SyslogMessage();

            final ByteArrayOutputStream outStream = new ByteArrayOutputStream(syslogMessage.length());
            final GZIPOutputStream gzip = new GZIPOutputStream(outStream);
            gzip.write(syslogMessage.getBytes());
            gzip.close();

            final ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

            // upload as file
            amazonS3.putObject(bucket, path, inStream, null);
            rows++;
        }

        return rows;
    }
}
