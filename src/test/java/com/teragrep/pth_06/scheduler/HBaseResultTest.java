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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public final class HBaseResultTest {

    @Test
    public void testToBatchUnitConvertsCorrectly() {
        Result result = createResult("1", false);
        HBaseResult hBaseResult = new HBaseResult(result);
        BatchUnit batchUnit = hBaseResult.toBatchUnit();
        ArchiveS3ObjectMetadata metadata = batchUnit.archiveS3ObjectMetadata;
        Assertions.assertEquals("1", metadata.id);
        Assertions.assertEquals("dir", metadata.directory);
        Assertions.assertEquals("stream", metadata.stream);
        Assertions.assertEquals("host", metadata.host);
        Assertions.assertEquals("bucket", metadata.bucket);
        Assertions.assertEquals("path", metadata.path);
        Assertions.assertEquals(123456789L, metadata.logtimeEpoch);
        Assertions.assertEquals(1000L, metadata.compressedSize);
        Assertions.assertEquals(10000L, metadata.uncompressedSize);
    }

    @Test
    public void testNullUncompressedSizeReturnMinusOne() {
        Result result = createResult("2", true);
        HBaseResult hBaseResult = new HBaseResult(result);
        BatchUnit batchUnit = hBaseResult.toBatchUnit();
        ArchiveS3ObjectMetadata metadata = batchUnit.archiveS3ObjectMetadata;
        Assertions.assertEquals(-1L, metadata.uncompressedSize);
    }

    private Result createResult(final String id, final boolean isUncompressedNull) {
        final byte[] family = Bytes.toBytes("meta");
        final Put put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("row1"));
        put.addColumn(family, Bytes.toBytes("i"), Bytes.toBytes(id));
        put.addColumn(family, Bytes.toBytes("d"), Bytes.toBytes("dir"));
        put.addColumn(family, Bytes.toBytes("s"), Bytes.toBytes("stream"));
        put.addColumn(family, Bytes.toBytes("h"), Bytes.toBytes("host"));
        put.addColumn(family, Bytes.toBytes("b"), Bytes.toBytes("bucket"));
        put.addColumn(family, Bytes.toBytes("p"), Bytes.toBytes("path"));
        put.addColumn(family, Bytes.toBytes("t"), Bytes.toBytes(123456789L));
        put.addColumn(family, Bytes.toBytes("fs"), Bytes.toBytes(1000L));
        if (isUncompressedNull) {
            put.addColumn(family, Bytes.toBytes("ufs"), new byte[0]);
        }
        else {
            put.addColumn(family, Bytes.toBytes("ufs"), Bytes.toBytes(10000L));
        }
        final List<Cell> cells = put
                .getFamilyCellMap()
                .values()
                .stream()
                .flatMap(List::stream)
                .sorted(org.apache.hadoop.hbase.CellComparator.getInstance())
                .collect(Collectors.toList());
        return Result.create(cells);
    }
}
