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
import com.teragrep.pth_06.KafkaTopicPartitionOffsetMetadata;

import java.io.Serializable;

/**
 * <h1>Batch Slice</h1> Class for representing a serializable batch slice. Can be constructed using S3 or Kafka
 * partition metadata.
 *
 * @see ArchiveS3ObjectMetadata
 * @see KafkaTopicPartitionOffsetMetadata
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public final class BatchSlice implements Serializable {

    public enum Type {
        ARCHIVE, KAFKA
    }

    public final Type type;
    public final ArchiveS3ObjectMetadata archiveS3ObjectMetadata;
    public final KafkaTopicPartitionOffsetMetadata kafkaTopicPartitionOffsetMetadata;

    public BatchSlice(ArchiveS3ObjectMetadata archiveS3ObjectMetadata) {
        this.type = Type.ARCHIVE;
        this.archiveS3ObjectMetadata = archiveS3ObjectMetadata;
        this.kafkaTopicPartitionOffsetMetadata = null;
    }

    public BatchSlice(KafkaTopicPartitionOffsetMetadata kafkaTopicPartitionOffsetMetadata) {
        this.type = Type.KAFKA;
        this.archiveS3ObjectMetadata = null;
        this.kafkaTopicPartitionOffsetMetadata = kafkaTopicPartitionOffsetMetadata;
    }

    public long getSize() {
        // FIXME compressed size to realsize estimate
        switch (type) {
            case ARCHIVE:
                // FIXME: Dummy metadata object if not present?
                if (archiveS3ObjectMetadata != null) {
                    return archiveS3ObjectMetadata.compressedSize;
                }
                else {
                    throw new RuntimeException("Expected archive s3 object metadata, instead was null");
                }
            case KAFKA:
                // TODO estimate based on offset delta
                return 1024 * 1024 * 16;
            default:
                throw new IllegalStateException("unknown BatchSliceType " + type);
        }
    }

    @Override
    public String toString() {
        return "BatchSlice{" + "batchSliceType=" + type + ", archiveS3ObjectMetadata=" + archiveS3ObjectMetadata
                + ", kafkaTopicPartitionOffsetMetadata=" + kafkaTopicPartitionOffsetMetadata + '}';
    }
}
