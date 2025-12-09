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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;

public final class S3ObjectHeaderTest {

    @Test
    public void validTimestampTest() {
        final String timestamp = "2025-12-08T12:34:56Z";
        final InputStream stream = new ByteArrayInputStream(timestamp.getBytes());
        final S3ObjectHeader header = new S3ObjectHeader(stream);
        Assertions.assertTrue(header.isValid(), "Header should be valid");
        Assertions
                .assertEquals(Instant.parse(timestamp).getEpochSecond(), header.epoch(), "Epoch seconds should match");
    }

    @Test
    public void fullSysLogHeaderTest() {
        final String headerLine = "2025-12-08T12:34:56Z myhost appname 1234 ID47 [meta info] Message text here";
        final InputStream stream = new ByteArrayInputStream(headerLine.getBytes());
        final S3ObjectHeader header = new S3ObjectHeader(stream);
        long expectedEpoch = Instant.parse("2025-12-08T12:34:56Z").getEpochSecond();
        Assertions.assertEquals(expectedEpoch, header.epoch());
    }

    @Test
    public void nilTimestampTest() {
        final String nilTimestamp = "-";
        final InputStream stream = new ByteArrayInputStream(nilTimestamp.getBytes());
        final S3ObjectHeader header = new S3ObjectHeader(stream);
        Assertions.assertTrue(header.isValid(), "Nil timestamp should be valid");
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, header::epoch);
        final String expected = "Cannot extract epoch, timestamp value was nil (-)";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    public void invalidTimestampTest() {
        final String invalid = "not-a-timestamp";
        final InputStream stream = new ByteArrayInputStream(invalid.getBytes());
        final S3ObjectHeader header = new S3ObjectHeader(stream);
        Assertions.assertFalse(header.isValid(), "Invalid timestamp should not be valid");
        final IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, header::epoch);
        final String expected = "Cannot extract a valid timestamp from header";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    public void emptyStreamTest() {
        final InputStream emptyStream = new ByteArrayInputStream(new byte[0]);
        final RuntimeException exception = Assertions
                .assertThrows(RuntimeException.class, () -> new S3ObjectHeader(emptyStream));
        final String expected = "No bytes could be read from the input stream";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    public void timestampWithOffsetToEpochTest() {
        final String timestamp = "2025-12-08T15:34:56+03:00";
        final InputStream stream = new ByteArrayInputStream(timestamp.getBytes());
        final S3ObjectHeader header = new S3ObjectHeader(stream);
        final long expectedEpoch = Instant.parse("2025-12-08T12:34:56Z").getEpochSecond();
        Assertions.assertEquals(expectedEpoch, header.epoch());
    }

    @Test
    public void partiallyMalformedTimestampTest() {
        final String malformed = "2025-12-08T25:61:61Z";
        final InputStream stream = new ByteArrayInputStream(malformed.getBytes());
        final S3ObjectHeader header = new S3ObjectHeader(stream);
        Assertions.assertFalse(header.isValid(), "Malformed timestamp should be invalid");
        Assertions.assertThrows(IllegalStateException.class, header::epoch);
    }
}
