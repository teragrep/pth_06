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
package com.teragrep.pth_06.task.hdfs;

import com.teragrep.pth_06.HdfsFileMetadata;
import com.teragrep.pth_06.avro.SyslogRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

// This class will read the records from avro-files fetched from HDFS with the help of AvroReadImpl and convert them to InternalRow used by pth_06.
public final class HdfsRecordConversionImpl implements HdfsRecordConversion {

    final Logger LOGGER = LoggerFactory.getLogger(HdfsRecordConversionImpl.class);

    private final LinkedList<AvroReadImpl> reads;
    private final LinkedList<HdfsFileMetadata> taskObjectList;
    private final long cutoffEpoch;
    private final FileSystem fs;

    public HdfsRecordConversionImpl(FileSystem fs, LinkedList<HdfsFileMetadata> taskObjectList, long cutoffEpoch) {
        this.fs = fs;
        this.taskObjectList = taskObjectList;
        this.reads = new LinkedList<>();
        this.cutoffEpoch = cutoffEpoch;
    }

    @Override
    public boolean next() throws IOException {
        // Load the taskObjects from taskObjectList to reads list.
        if (reads.isEmpty() && !taskObjectList.isEmpty()) {
            open();
        }

        boolean hasNext = false;
        while (!hasNext && !reads.isEmpty()) {
            hasNext = reads.getFirst().next();
            if (!hasNext) {
                reads.getFirst().close();
                reads.removeFirst();
            }
            else {
                // Time based inclusion WIP
                /*SyslogRecord record = reads.getFirst().record();
                if (record.getTimestamp() < cutoffEpoch) {
                    hasNext = false;
                }*/
            }
        }
        return hasNext;
    }

    @Override
    public InternalRow row() {
        SyslogRecord currentRecord = reads.getFirst().record();
        UnsafeRowWriter rowWriter = new UnsafeRowWriter(11);
        rowWriter.reset();
        rowWriter.zeroOutNullBytes();
        rowWriter.write(0, currentRecord.getTimestamp());
        rowWriter.write(1, UTF8String.fromString(currentRecord.getPayload().toString()));
        rowWriter.write(2, UTF8String.fromString(currentRecord.getDirectory().toString()));
        rowWriter.write(3, UTF8String.fromString(currentRecord.getStream().toString()));
        rowWriter.write(4, UTF8String.fromString(currentRecord.getHost().toString()));
        rowWriter.write(5, UTF8String.fromString(currentRecord.getInput().toString()));
        rowWriter.write(6, UTF8String.fromString(currentRecord.getPartition().toString()));
        rowWriter.write(7, currentRecord.getOffset());
        rowWriter.write(8, UTF8String.fromString(currentRecord.getOrigin().toString()));
        return rowWriter.getRow();
    }

    private void open() throws IOException {
        for (HdfsFileMetadata taskObject : taskObjectList) {
            reads.add(new AvroReadImpl(fs, taskObject));
        }
        taskObjectList.clear();
    }

}
