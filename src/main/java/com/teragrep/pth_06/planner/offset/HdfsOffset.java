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
package com.teragrep.pth_06.planner.offset;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

// Class for representing a serializable offset of HDFS data source.
// S3 has epoch hours as offsets, kafka has native TopicPartition offsets and HDFS should have file-metadata (use same format as in Kafka, topicpartition + record offset, which can be extracted from the metadata).

public class HdfsOffset extends Offset implements Serializable {

    // TODO: Implement everything that is needed for tracking the offsets for HDFS datasource.

    private static final Type mapType = new TypeToken<Map<String, Long>>() {
    }.getType();
    private final Map<String, Long> serializedHdfsOffset;

    public HdfsOffset(Map<TopicPartition, Long> offset) {
        serializedHdfsOffset = new HashMap<>(offset.size());
        for (Map.Entry<TopicPartition, Long> entry : offset.entrySet()) {

            serializedHdfsOffset.put(entry.getKey().toString(), entry.getValue()); // offset
        }
    }

    // TODO: Check if these methods originating from KafkaOffset can be implemented as-is or not.
    public HdfsOffset(String s) {
        Gson gson = new Gson();
        serializedHdfsOffset = gson.fromJson(s, mapType);
    }

    public Map<TopicPartition, Long> getOffsetMap() {
        Map<TopicPartition, Long> rv = new HashMap<>(serializedHdfsOffset.size());

        for (Map.Entry<String, Long> entry : serializedHdfsOffset.entrySet()) {
            String topicAndPartition = entry.getKey();
            long offset = entry.getValue();

            int splitterLocation = topicAndPartition.lastIndexOf('-');
            int partition = Integer.parseInt(topicAndPartition.substring(splitterLocation + 1));
            String topic = topicAndPartition.substring(0, splitterLocation);
            rv.put(new TopicPartition(topic, partition), offset);
        }

        return rv;
    }

    @Override
    public String json() {
        Gson gson = new Gson();
        return gson.toJson(serializedHdfsOffset);
    }

    @Override
    public String toString() {
        return "HdfsOffset{" + "serializedHdfsOffset=" + serializedHdfsOffset + '}';
    }
}