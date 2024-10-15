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
package com.teragrep.pth_06.task;

import com.teragrep.pth_06.FileSystemFactoryImpl;
import com.teragrep.pth_06.HdfsFileMetadata;
import com.teragrep.pth_06.task.hdfs.HdfsRecordConversionImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

public final class HdfsMicroBatchInputPartitionReader implements PartitionReader<InternalRow> {

    final Logger LOGGER = LoggerFactory.getLogger(HdfsMicroBatchInputPartitionReader.class);

    private final LinkedList<HdfsFileMetadata> taskObjectList;
    private final FileSystem fs;
    private final long cutoffEpoch;
    private HdfsRecordConversionImpl hdfsRecordConversionImpl;

    public HdfsMicroBatchInputPartitionReader(
            long cutoffEpoch,
            String kerberosAuthentication,
            String hdfsUri,
            String hdfsPath,
            String kerberosRealm,
            String kerberosKdc,
            String kerberosAuthorization,
            String kerberosAutorenewal,
            String UseHdfsHostname,
            String kerberosPrincipalPattern,
            String hdfsTransferProtection,
            String hdfsCipherSuites,
            String kerberosKeytabUser,
            String kerberosKeytabPath,
            LinkedList<HdfsFileMetadata> taskObjectList
    ) throws IOException {

        this.cutoffEpoch = cutoffEpoch;
        this.taskObjectList = taskObjectList;

        fs = new FileSystemFactoryImpl(
                kerberosAuthentication,
                hdfsUri,
                kerberosRealm,
                kerberosKdc,
                kerberosAuthorization,
                kerberosAutorenewal,
                UseHdfsHostname,
                kerberosPrincipalPattern,
                hdfsTransferProtection,
                hdfsCipherSuites,
                kerberosKeytabUser,
                kerberosKeytabPath
        ).fileSystem(false);

        this.hdfsRecordConversionImpl = new HdfsRecordConversionImpl(fs); // stub
    }

    // Read avro-file until it ends
    @Override
    public boolean next() throws IOException {
        // true if data is available, false if not
        boolean rv = false;

        while (!taskObjectList.isEmpty() && !rv) {
            if (hdfsRecordConversionImpl.isStub()) {
                hdfsRecordConversionImpl = new HdfsRecordConversionImpl(fs, taskObjectList.getFirst());
                hdfsRecordConversionImpl.open();
            }

            rv = hdfsRecordConversionImpl.next();

            if (!rv) {
                // object was consumed
                hdfsRecordConversionImpl.close();

                // remove consumed object
                taskObjectList.removeFirst();
                if (!taskObjectList.isEmpty()) {
                    hdfsRecordConversionImpl = new HdfsRecordConversionImpl(fs, taskObjectList.getFirst());
                    hdfsRecordConversionImpl.open();
                }
            }
            else {
                // time based inclusion, skip record and continue loop if the record is older than cutoffEpoch.
                long rfc5424time = hdfsRecordConversionImpl.get().getLong(0); // timestamp as epochMicros
                if (rfc5424time < cutoffEpoch) {
                    rv = false;
                }
            }
        }
        LOGGER.debug("next rv: <{}>", rv);
        return rv;
    }

    @Override
    public InternalRow get() {
        InternalRow rv = hdfsRecordConversionImpl.get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("get(): <{}>", rv.getLong(7));
        }
        return rv;
    }

    @Override
    public void close() throws IOException {
        LOGGER.debug("HdfsMicroBatchInputPartitionReader.close");
        if (!hdfsRecordConversionImpl.isStub()) {
            hdfsRecordConversionImpl.close();
        }
    }
}
