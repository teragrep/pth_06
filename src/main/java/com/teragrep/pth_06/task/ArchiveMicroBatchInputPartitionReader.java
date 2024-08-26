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

import com.amazonaws.services.s3.AmazonS3;
import com.teragrep.pth_06.ArchiveS3ObjectMetadata;
import com.teragrep.pth_06.task.s3.Pth06S3Client;
import com.teragrep.pth_06.task.s3.RowConverter;
import com.teragrep.rad_01.AuditPlugin;
import com.teragrep.rad_01.AuditPluginFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;

// AWS-client

// logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// rfc5424

/**
 * <h1>Archive Micro Batch Input Partition Reader</h1> Class for holding micro batch partition of a RFC5424 syslog data.
 *
 * @see PartitionReader
 * @since 02/03/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 */
class ArchiveMicroBatchInputPartitionReader implements PartitionReader<InternalRow> {

    final Logger LOGGER = LoggerFactory.getLogger(ArchiveMicroBatchInputPartitionReader.class);

    private final AuditPlugin auditPlugin;
    private RowConverter rowConverter;

    private final LinkedList<ArchiveS3ObjectMetadata> taskObjectList;
    private final AmazonS3 s3client;

    private final boolean skipNonRFC5424Files;

    public ArchiveMicroBatchInputPartitionReader(
            String S3endPoint,
            String S3identity,
            String S3credential,
            LinkedList<ArchiveS3ObjectMetadata> taskObjectList,
            String TeragrepAuditQuery,
            String TeragrepAuditReason,
            String TeragrepAuditUser,
            String TeragrepAuditPluginClassName,
            boolean skipNonRFC5424Files
    ) {
        this.taskObjectList = taskObjectList;

        this.s3client = new Pth06S3Client(S3endPoint, S3identity, S3credential).build();

        AuditPluginFactory auditPluginFactory = new AuditPluginFactory(TeragrepAuditPluginClassName);

        try {
            this.auditPlugin = auditPluginFactory.getAuditPlugin();
            this.auditPlugin.setQuery(TeragrepAuditQuery);
            this.auditPlugin.setReason(TeragrepAuditReason);
            this.auditPlugin.setUser(TeragrepAuditUser);

        }
        catch (
                ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException
                | NoSuchMethodException e
        ) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        this.skipNonRFC5424Files = skipNonRFC5424Files;
    }

    // read zip until it ends
    @Override
    public boolean next() throws IOException {
        // true if data is available, false if not
        boolean rv = false;

        while (!taskObjectList.isEmpty() && !rv) {
            // loop until all objects are consumed
            if (rowConverter == null) {
                // initial run
                rowConverter = new RowConverter(
                        auditPlugin,
                        s3client,
                        taskObjectList.getFirst().id,
                        taskObjectList.getFirst().bucket,
                        taskObjectList.getFirst().path,
                        taskObjectList.getFirst().directory,
                        taskObjectList.getFirst().stream,
                        taskObjectList.getFirst().host,
                        skipNonRFC5424Files
                );
                rowConverter.open();
            }

            /*
            there is an object available
            purpose of the while loop is to get here if the object was done
             */
            rv = rowConverter.next();

            if (!rv) {
                // object was consumed
                rowConverter.close();

                // remove consumed object
                taskObjectList.removeFirst();

                if (!taskObjectList.isEmpty()) {
                    // new object still available
                    rowConverter = new RowConverter(
                            auditPlugin,
                            s3client,
                            taskObjectList.getFirst().id,
                            taskObjectList.getFirst().bucket,
                            taskObjectList.getFirst().path,
                            taskObjectList.getFirst().directory,
                            taskObjectList.getFirst().stream,
                            taskObjectList.getFirst().host,
                            skipNonRFC5424Files
                    );
                    rowConverter.open();
                }
            }
        }

        return rv;
    }

    @Override
    public InternalRow get() {
        return rowConverter.get();
    }

    @Override
    public void close() throws IOException {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("ArchiveMicroBatchInputPartitionReader.close>");
        if (rowConverter != null) {
            rowConverter.close();
        }
    }
}
