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

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <h1>Kafka Micro Batch Input Partition</h1> Class for holding micro batch partition of a kafka topic.
 *
 * @see InputPartition
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class KafkaMicroBatchInputPartition implements InputPartition {

    public final Logger LOGGER = LoggerFactory.getLogger(KafkaMicroBatchInputPartition.class);

    public final Map<String, Object> executorKafkaProperties;
    public final TopicPartition topicPartition;
    public final long startOffset;
    public final long endOffset;
    public final Map<String, String> executorConfig;
    public final boolean skipNonRFC5424Records;

    public KafkaMicroBatchInputPartition(
            Map<String, Object> executorKafkaProperties,
            TopicPartition topicPartition,
            long startOffset,
            long endOffset,
            Map<String, String> executorConfig,
            boolean skipNonRFC5424Records
    ) {
        this.executorKafkaProperties = executorKafkaProperties;
        this.topicPartition = topicPartition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.executorConfig = executorConfig;
        this.skipNonRFC5424Records = skipNonRFC5424Records;
    }
}
