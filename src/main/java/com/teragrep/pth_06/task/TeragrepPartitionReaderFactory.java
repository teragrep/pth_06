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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * <h1>TeragrepPartitionReaderFactory</h1> Used to create appropriate PartitionReaders based on the type of the
 * InputPartition provided.
 * 
 * @author p000043u
 */
public final class TeragrepPartitionReaderFactory implements PartitionReaderFactory {

    private static final long serialVersionUID = 1L;
    public final boolean isMetadataQuery;

    public TeragrepPartitionReaderFactory(boolean isMetadataQuery) {
        super();
        this.isMetadataQuery = isMetadataQuery;
    }

    /**
     * Creates a PartitionReader of the appropriate type based on the InputPartition type.
     * 
     * @param inputPartition InputPartition of type Archive or Kafka
     * @return PartitionReader appropriate for the given InputPartition type
     * @throws RuntimeException If InputPartition is of unknown type
     */
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        // Use different PartitionReaders based on InputPartition type (Archive or Kafka)
        if (isMetadataQuery && inputPartition instanceof ArchiveMicroBatchInputPartition) {
            // metadata only
            ArchiveMicroBatchInputPartition aip = (ArchiveMicroBatchInputPartition) inputPartition;
            return new MetadataMicroBatchInputPartitionReader(
                    aip.taskObjectList,
                    aip.TeragrepAuditQuery,
                    aip.TeragrepAuditReason,
                    aip.TeragrepAuditUser,
                    aip.TeragrepAuditPluginClassName
            );
        }
        else if (inputPartition instanceof ArchiveMicroBatchInputPartition) {
            ArchiveMicroBatchInputPartition aip = (ArchiveMicroBatchInputPartition) inputPartition;
            return new ArchiveMicroBatchInputPartitionReader(
                    aip.S3endPoint,
                    aip.S3identity,
                    aip.S3credential,
                    aip.taskObjectList,
                    aip.TeragrepAuditQuery,
                    aip.TeragrepAuditReason,
                    aip.TeragrepAuditUser,
                    aip.TeragrepAuditPluginClassName,
                    aip.skipNonRFC5424Files
            );
        }
        else if (inputPartition instanceof KafkaMicroBatchInputPartition) {
            KafkaMicroBatchInputPartition kip = (KafkaMicroBatchInputPartition) inputPartition;
            return new KafkaMicroBatchInputPartitionReader(
                    kip.executorKafkaProperties,
                    kip.topicPartition,
                    kip.startOffset,
                    kip.endOffset,
                    kip.executorConfig,
                    kip.skipNonRFC5424Records
            );
        }

        // inputPartition is neither Archive nor Kafka type
        throw new RuntimeException(
                "Invalid input partition type provided to ArchivePartitionReaderFactory: "
                        + inputPartition.getClass().getName()
        );
    }
}
