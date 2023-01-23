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

package com.teragrep.pth06.scheduler;

import com.teragrep.pth06.ArchiveS3ObjectMetadata;
import com.teragrep.pth06.KafkaTopicPartitionOffsetMetadata;

import java.io.Serializable;

/**
 * <h1>Batch Slice</h1>
 *
 * Class for representing a serializable batch slice.
 * Can be constructed using S3 or Kafka partition metadata.
 *
 * @see ArchiveS3ObjectMetadata
 * @see KafkaTopicPartitionOffsetMetadata
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class BatchSlice implements Serializable {

    private final BatchSliceType batchSliceType;
    private final ArchiveS3ObjectMetadata archiveS3ObjectMetadata;
    private final KafkaTopicPartitionOffsetMetadata kafkaTopicPartitionOffsetMetadata;

    public BatchSlice(ArchiveS3ObjectMetadata archiveS3ObjectMetadata) {
        this.batchSliceType = BatchSliceType.ARCHIVE;
        this.archiveS3ObjectMetadata = archiveS3ObjectMetadata;
        this.kafkaTopicPartitionOffsetMetadata = null;
    }

    public BatchSlice(KafkaTopicPartitionOffsetMetadata kafkaTopicPartitionOffsetMetadata) {
        this.batchSliceType = BatchSliceType.KAFKA;
        this.archiveS3ObjectMetadata = null;
        this.kafkaTopicPartitionOffsetMetadata = kafkaTopicPartitionOffsetMetadata;
    }

    public BatchSliceType getBatchSliceType() {
        return batchSliceType;
    }

    public ArchiveS3ObjectMetadata getArchiveS3ObjectMetadata() {
        if (batchSliceType != BatchSliceType.ARCHIVE) {
            throw new IllegalArgumentException("batchSliceType != BatchSliceType.ARCHIVE");
        }
        return archiveS3ObjectMetadata;
    }

    public KafkaTopicPartitionOffsetMetadata getKafkaTopicPartitionOffsetMetadata() {
        if (batchSliceType != BatchSliceType.KAFKA) {
            throw new IllegalArgumentException("batchSliceType != BatchSliceType.KAFKA");
        }
        return kafkaTopicPartitionOffsetMetadata;
    }

    public long getSize() {
        // FIXME compressed size to realsize estimate
        switch (batchSliceType) {
            case ARCHIVE:
                return archiveS3ObjectMetadata.getCompressedSize();
            case KAFKA:
                // TODO estimate based on offset delta
                return 1024*1024*16;
            default:
                throw new IllegalStateException("unknown BatchSliceType " + batchSliceType);
        }
    }

    @Override
    public String toString() {
        return "BatchSlice{" +
                "batchSliceType=" + batchSliceType +
                ", archiveS3ObjectMetadata=" + archiveS3ObjectMetadata +
                ", kafkaTopicPartitionOffsetMetadata=" + kafkaTopicPartitionOffsetMetadata +
                '}';
    }
}
