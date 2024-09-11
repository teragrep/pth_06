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

    private static final Type mapType = new TypeToken<Map<String, Long>>() {}.getType();
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
        return "HdfsOffset{" +
                "serializedHdfsOffset=" + serializedHdfsOffset +
                '}';
    }
}