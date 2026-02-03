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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.teragrep.pth_06.MockS3Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.zip.GZIPOutputStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationRowConverterTest {

    private final String bucket = "bucket";
    private final String path = "2007/10-08/epoch/migration/test.logGLOB-2007100814.log.gz";
    private final MockS3Configuration mockS3Configuration = new MockS3Configuration(
            "http://127.0.0.1:48080",
            "s3identity",
            "s3credential"
    );
    private final MockS3 mockS3 = new MockS3(
            mockS3Configuration.s3endpoint(),
            mockS3Configuration.s3identity(),
            mockS3Configuration.s3credential()
    );
    private final AmazonS3 amazonS3 = new Pth06S3Client(
            mockS3Configuration.s3endpoint(),
            mockS3Configuration.s3identity(),
            mockS3Configuration.s3credential()
    ).build();

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(mockS3::start);
        ensureBucketExists();
    }

    @AfterAll
    public void close() {
        Assertions.assertDoesNotThrow(mockS3::stop);
    }

    @Test
    public void testSyslogConversion() {
        final String syslog = "<14>1 2014-06-20T09:14:07.12345+00:00 host01 systemd DEA MSG-01 [sd_one@48577 id_one=\"eno\" id_two=\"owt\"][sd_two@48577 id_three=\"eerht\" id_four=\"ruof\"] msg\n";
        Assertions.assertDoesNotThrow(() -> load(syslog));
        final RowConverter rowConverter = new EpochMigrationRowConverter(
                amazonS3,
                "id",
                bucket,
                path,
                "directory",
                "stream",
                "host"
        );
        Assertions.assertDoesNotThrow(rowConverter::open);
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertTrue(rowConverter.next(), "first call should succeed");
        });
        final InternalRow internalRow = rowConverter.get();

        // Assuming that this timestamp is in microseconds
        // GMT: Friday, June 20, 2014 9:14:07.123 AM
        final long time = internalRow.getLong(0);
        Assertions.assertEquals(1403255647123450L, time);

        final String raw = internalRow.getString(1);
        final String expectedRaw = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"2007/10-08/epoch/migration/test.logGLOB-2007100814.log.gz\",\"partition\":\"id\"},\"timestamp\":{\"rfc5242timestamp\":\"2014-06-20T09:14:07.12345+00:00\",\"epoch\":1403255647123450,\"path-extracted\":\"2007-10-08T14:00+03:00[Europe/Helsinki]\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
        Assertions.assertEquals(expectedRaw, raw);

        Assertions
                .assertDoesNotThrow(
                        () -> Assertions
                                .assertFalse(rowConverter.next(), "no more rows available after first read attempt")
                );
        Assertions.assertDoesNotThrow(rowConverter::close);
    }

    @Test
    public void testNonSyslogRow() {
        final String syslog = "non/syslog/format/event";
        Assertions.assertDoesNotThrow(() -> load(syslog));
        final RowConverter rowConverter = new EpochMigrationRowConverter(
                amazonS3,
                "id",
                bucket,
                path,
                "directory",
                "stream",
                "host"
        );
        Assertions.assertDoesNotThrow(rowConverter::open);
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertTrue(rowConverter.next(), "first call should succeed");
        });
        final InternalRow internalRow = rowConverter.get();

        // Path extracted date from 2007100814 with hourly date
        // Archive assumes Europe/Helsinki, so we use ZonedDateTime to account for DST in assertions
        // _time
        long time = internalRow.getLong(0);
        long expectedEpochSeconds = ZonedDateTime
                .of(2007, 10, 8, 14, 0, 0, 0, ZoneId.of("Europe/Helsinki"))
                .toEpochSecond();
        Assertions.assertEquals(expectedEpochSeconds * 1000 * 1000, time);

        final String raw = internalRow.getString(1);
        final String expectedRaw = "{\"epochMigration\":true,\"format\":\"non-rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"2007/10-08/epoch/migration/test.logGLOB-2007100814.log.gz\",\"partition\":\"id\"},\"timestamp\":{\"path-extracted\":\"2007-10-08T14:00+03:00[Europe/Helsinki]\",\"path-extracted-precision\":\"hourly\",\"source\":\"object-path\"}}";
        Assertions.assertEquals(expectedRaw, raw);

        Assertions
                .assertDoesNotThrow(
                        () -> Assertions
                                .assertFalse(rowConverter.next(), "no more rows available after first read attempt")
                );
        Assertions.assertDoesNotThrow(rowConverter::close);
    }

    @Test
    public void testEmptyFile() {
        Assertions.assertDoesNotThrow(() -> load(""));
        RowConverter rowConverter = new EpochMigrationRowConverter(
                amazonS3,
                "id",
                bucket,
                path,
                "directory",
                "stream",
                "host"
        );
        Assertions.assertDoesNotThrow(rowConverter::open);
        Assertions.assertDoesNotThrow(() -> Assertions.assertFalse(rowConverter.next()));
        Assertions.assertDoesNotThrow(rowConverter::close);
    }

    private void load(final String content) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
        }
        final ByteArrayInputStream inStream = new ByteArrayInputStream(baos.toByteArray());
        amazonS3.putObject(bucket, path, inStream, null);
    }

    private void ensureBucketExists() {
        boolean bucketExists = false;
        for (final Bucket existingBucket : amazonS3.listBuckets()) {
            if (existingBucket.getName().equals(bucket)) {
                bucketExists = true;
                break;
            }
        }
        if (!bucketExists) {
            amazonS3.createBucket(bucket);
        }
    }
}
