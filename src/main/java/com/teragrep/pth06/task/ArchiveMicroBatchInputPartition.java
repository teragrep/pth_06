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

package com.teragrep.pth06.task;

import com.teragrep.pth06.ArchiveS3ObjectMetadata;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

// logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * <h2>Kafka Micro Batch Input Partition Reader</h2>
 *
 * Class for holding micro batch partition of RFC5424 syslog data.
 *
 * @see InputPartition
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class ArchiveMicroBatchInputPartition implements InputPartition<InternalRow> {
    final Logger LOGGER = LoggerFactory.getLogger(ArchiveMicroBatchInputPartition.class);

    private final String S3endPoint;
    private final String S3identity ;
    private final String S3credential;
    private final LinkedList<ArchiveS3ObjectMetadata> taskObjectList;

    private final String TeragrepAuditQuery;
    private final String TeragrepAuditReason;
    private final String TeragrepAuditUser;
    private final String TeragrepAuditPluginClassName;

    private final boolean skipNonRFC5424Files;


    public ArchiveMicroBatchInputPartition(String S3endPoint,
                                           String S3identity,
                                           String S3credential,
                                           LinkedList<ArchiveS3ObjectMetadata> taskObjectList,
                                           String TeragrepAuditQuery,
                                           String TeragrepAuditReason,
                                           String TeragrepAuditUser,
                                           String TeragrepAuditPluginClassName,
                                           boolean skipNonRFC5424Files
    )
                                       {
        if(LOGGER.isDebugEnabled())
            LOGGER.debug("ArchiveMicroBatchInputPartition> init");

        this.S3endPoint = S3endPoint;
        this.S3identity = S3identity;
        this.S3credential = S3credential;

        this.taskObjectList = taskObjectList;

        this.TeragrepAuditQuery = TeragrepAuditQuery;
        this.TeragrepAuditReason = TeragrepAuditReason;
        this.TeragrepAuditUser = TeragrepAuditUser;
        this.TeragrepAuditPluginClassName = TeragrepAuditPluginClassName;

        this.skipNonRFC5424Files = skipNonRFC5424Files;
    }

    @Override
    public String[] preferredLocations() {
        if(LOGGER.isDebugEnabled())
            LOGGER.debug("ArchiveMicroBatchInputPartition.preferredLocations>");
        String[] loc = new String[1];
        // we have none
        loc[0] = "_";
        return loc;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        if(LOGGER.isDebugEnabled())
            LOGGER.debug("ArchiveMicroBatchInputPartition.createPartitionReader>  partition: " + this.taskObjectList);
        return new ArchiveMicroBatchInputPartitionReader(
                this.S3endPoint,
                this.S3identity,
                this.S3credential,
                this.taskObjectList,
                this.TeragrepAuditQuery,
                this.TeragrepAuditReason,
                this.TeragrepAuditUser,
                this.TeragrepAuditPluginClassName,
                this.skipNonRFC5424Files
       );
    }

    @Override
    public String toString() {
        return "ArchiveMicroBatchInputPartition{" +
                "S3endPoint='" + S3endPoint + '\'' +
                ", S3identity='" + S3identity + '\'' +
                ", S3credential='" + S3credential + '\'' +
                ", taskObjectList=" + taskObjectList +
                ", TeragrepAuditQuery='" + TeragrepAuditQuery + '\'' +
                ", TeragrepAuditReason='" + TeragrepAuditReason + '\'' +
                ", TeragrepAuditUser='" + TeragrepAuditUser + '\'' +
                ", TeragrepAuditPluginClassName='" + TeragrepAuditPluginClassName + '\'' +
                '}';
    }
}
