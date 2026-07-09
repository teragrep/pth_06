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
package com.teragrep.pth_06.scheduler;

import com.teragrep.pth_06.ArchiveS3ObjectMetadata;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

final class HBaseResult {

    private final Result result;
    private final byte[] columnFamilyBytes;

    HBaseResult(final Result result) {
        this(result, "meta");
    }

    HBaseResult(final Result result, final String columnFamilyName) {
        this(result, Bytes.toBytes(columnFamilyName));
    }

    HBaseResult(final Result result, byte[] columnFamilyBytes) {
        this.result = result;
        this.columnFamilyBytes = columnFamilyBytes;
    }

    BatchUnit toBatchUnit() {
        final String id = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("i")));
        final String directory = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("d")));
        final String stream = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("s")));
        final String host = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("h")));
        final String bucket = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("b")));
        final String path = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("p")));
        final long logtime = Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("t")));
        final long filesize = Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("fs")));
        final byte[] uncompressedFileSizeBytes = result.getValue(columnFamilyBytes, Bytes.toBytes("ufs"));
        final long uncompressedFileSize;
        // uncompressed can be null, null values are replicated as empty byte arrays to hbase
        if (uncompressedFileSizeBytes.length == 0) {
            uncompressedFileSize = -1L;
        }
        else {
            uncompressedFileSize = Bytes.toLong(uncompressedFileSizeBytes);
        }
        return new BatchUnit(
                new ArchiveS3ObjectMetadata(
                        id,
                        bucket,
                        path,
                        directory,
                        stream,
                        host,
                        logtime,
                        filesize,
                        uncompressedFileSize
                )
        );
    }
}
