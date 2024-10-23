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
package com.teragrep.pth_06.planner;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.teragrep.pth_06.HdfsFileMetadata;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.offset.HdfsOffset;
import com.teragrep.pth_06.planner.walker.HdfsConditionWalker;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

// Class for processing hdfs queries.
public class HdfsQueryProcessor implements HdfsQuery {

    private final Logger LOGGER = LoggerFactory.getLogger(HdfsQueryProcessor.class);
    private LinkedList<HdfsFileMetadata> topicPartitionList;
    private final HdfsDBClient hdfsDBClient;
    private final Map<TopicPartition, Long> hdfsOffsetMap;
    private final Map<TopicPartition, Long> latestHdfsOffsetMap;
    private final long quantumLength;
    private final long numPartitions;
    private final long totalObjectCountLimit;
    private final boolean stub;

    public HdfsQueryProcessor() {
        totalObjectCountLimit = 0;
        hdfsDBClient = null; // refactor null to stub
        hdfsOffsetMap = new HashMap<>();
        latestHdfsOffsetMap = new HashMap<>();
        quantumLength = 0;
        numPartitions = 0;
        stub = true;
    }

    public HdfsQueryProcessor(Config config) {
        // get configs from config object
        this.quantumLength = config.batchConfig.quantumLength;
        this.numPartitions = config.batchConfig.numPartitions;
        this.totalObjectCountLimit = config.batchConfig.totalObjectCountLimit;
        // Filter only topics using regex pattern
        String topicsRegexString = "";
        if (config.query != null) {
            try {
                HdfsConditionWalker parser = new HdfsConditionWalker();
                topicsRegexString = parser.fromString(config.query);
            }
            catch (Exception e) {
                throw new RuntimeException(
                        "HdfsQueryProcessor problems when construction Query conditions query:" + config.query
                                + " exception:" + e
                );
            }
        }
        if (topicsRegexString.isEmpty()) {
            topicsRegexString = "^.*$"; // all topics if none given
        }
        // Implement hdfs db client that fetches the metadata for the files that are stored in hdfs based on topic name (aka. directory containing the files for a specific topic in HDFS).
        // Remember to implement Kerberized HDFS access for prod. Tests are done using normal access on mini cluster.
        try {
            this.hdfsDBClient = new HdfsDBClient(
                    config,
                    topicsRegexString // topicsRegexString only searches for the given topic/topics (aka. directories).
            );
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Generate the new starting partition offsets for kafka.
        hdfsOffsetMap = new HashMap<>(); // This parameter is used for generating the new start offsets for the KafkaOffsetPlanner. hdfsOffsetMapToJSON() is used to transfer the parameter using printer.
        seekToResults(); // This method loads all the available metadata to TopicPartitionList from HDFS.
        // Create a map that only contains the metadata with the highest offset for every topic partition.
        for (HdfsFileMetadata r : topicPartitionList) {
            long partitionStart = r.endOffset;
            if (!hdfsOffsetMap.containsKey(r.topicPartition)) {
                hdfsOffsetMap.put(r.topicPartition, partitionStart + 1);
            }
            else {
                if (hdfsOffsetMap.get(r.topicPartition) < partitionStart) {
                    hdfsOffsetMap.replace(r.topicPartition, partitionStart + 1);
                }
            }
        }
        latestHdfsOffsetMap = new HashMap<>();
        stub = false;
        LOGGER.debug("HdfsQueryProcessor.HdfsQueryProcessor>");
    }

    // pulls the metadata for available topic partition files in HDFS.
    private void seekToResults() {
        LOGGER.debug("HdfsQueryProcessor.seekToResults>");
        try {
            topicPartitionList = hdfsDBClient.pullToPartitionList(); // queries the list of topic partitions based on the topic name condition filtering.
            LOGGER.debug("HdfsQueryProcessor.seekToResults>");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Returns all the available HDFS file metadata between the given topic partition offsets.
    @Override
    public LinkedList<HdfsFileMetadata> processBetweenHdfsFileMetadata(HdfsOffset startOffset, HdfsOffset endOffset) {
        LinkedList<HdfsFileMetadata> rv = new LinkedList<>();
        Map<TopicPartition, Long> endOffsetMap = endOffset.getOffsetMap();
        Map<TopicPartition, Long> startOffsetMap = startOffset.getOffsetMap();
        for (HdfsFileMetadata r : topicPartitionList) {
            if (
                (endOffsetMap.get(r.topicPartition) >= r.endOffset)
                        & (startOffsetMap.get(r.topicPartition) <= r.endOffset)
            ) {
                rv.add(new HdfsFileMetadata(r.topicPartition, r.endOffset, r.hdfsFilePath, r.hdfsFileSize));
            }
        }
        return rv;
    }

    // Removes the committed topic partition offsets from topicPartitionList.
    @Override
    public void commit(HdfsOffset offset) {
        Map<TopicPartition, Long> offsetMap = offset.getOffsetMap();
        LinkedList<HdfsFileMetadata> newTopicPartitionList = new LinkedList<>();
        // Generate new topicPartitionList where the metadata with offset values lower than the offset values given as input parameter are filtered out.
        for (HdfsFileMetadata r : topicPartitionList) {
            if (offsetMap.get(r.topicPartition) < r.endOffset) {
                newTopicPartitionList
                        .add(new HdfsFileMetadata(r.topicPartition, r.endOffset, r.hdfsFilePath, r.hdfsFileSize));
            }
        }
        topicPartitionList = newTopicPartitionList;
    }

    // Prints json array containing the starting offsets for kafka to use.
    @Override
    public JsonArray hdfsOffsetMapToJSON() {
        JsonArray ja = new JsonArray();
        for (Map.Entry<TopicPartition, Long> entry : hdfsOffsetMap.entrySet()) {
            String topic = entry.getKey().topic();
            String partition = String.valueOf(entry.getKey().partition());
            String offset = String.valueOf(entry.getValue());
            JsonObject jo = new JsonObject();
            jo.addProperty("topic", topic);
            jo.addProperty("partition", partition);
            jo.addProperty("offset", offset);
            ja.add(jo);
        }
        return ja;
    }

    // returns the starting offsets for all available (aka. filtered) topic partitions.
    @Override
    public HdfsOffset getBeginningOffsets() {
        Map<TopicPartition, Long> startOffset = new HashMap<>();
        // Go through the topicPartitionList to generate start offsets.
        for (HdfsFileMetadata r : topicPartitionList) {
            // Generate startOffset
            // When going through the result, store the topic partition with the lowest offset to the startOffset object.
            long partitionOffset = r.endOffset;
            if (!startOffset.containsKey(r.topicPartition)) {
                startOffset.put(r.topicPartition, partitionOffset);
            }
            else {
                if (startOffset.get(r.topicPartition) > partitionOffset) {
                    startOffset.replace(r.topicPartition, partitionOffset);
                }
            }
        }
        Map<String, Long> serializedHdfsOffset = new HashMap<>(startOffset.size());
        for (Map.Entry<TopicPartition, Long> entry : startOffset.entrySet()) {
            serializedHdfsOffset.put(entry.getKey().toString(), entry.getValue()); // offset
        }
        return new HdfsOffset(serializedHdfsOffset);
    }

    // returns the end offsets for all available (aka. filtered) topic partitions.
    @Override
    public HdfsOffset getInitialEndOffsets() {
        Map<TopicPartition, Long> endOffset = new HashMap<>();
        // Go through the topicPartitionList to generate end offsets.
        for (HdfsFileMetadata r : topicPartitionList) {
            long partitionOffset = r.endOffset;
            // Generate endOffset
            // When going through the result, store the topic partition with the highest offset to the endOffset object.
            if (!endOffset.containsKey(r.topicPartition)) {
                endOffset.put(r.topicPartition, partitionOffset);
            }
            else {
                if (endOffset.get(r.topicPartition) < partitionOffset) {
                    endOffset.replace(r.topicPartition, partitionOffset);
                }
            }
        }
        Map<String, Long> serializedHdfsOffset = new HashMap<>(endOffset.size());
        for (Map.Entry<TopicPartition, Long> entry : endOffset.entrySet()) {
            serializedHdfsOffset.put(entry.getKey().toString(), entry.getValue()); // offset
        }
        return new HdfsOffset(serializedHdfsOffset);
    }

    // Increments the latest offset values and returns that incremented offsets. Works by pulling data from the topicPartitionList until weight limit is reached.
    @Override
    public HdfsOffset incrementAndGetLatestOffset() {
        if (this.latestHdfsOffsetMap.isEmpty()) {
            HdfsOffset beginningOffsets = getBeginningOffsets();
            this.latestHdfsOffsetMap.putAll(beginningOffsets.getOffsetMap());
        }
        // Initialize the batchSizeLimit object to split the data into appropriate sized batches
        BatchSizeLimit batchSizeLimit = new BatchSizeLimit(quantumLength * numPartitions, totalObjectCountLimit);
        // Keep loading more offsets from topicPartitionList until the limit is reached
        Iterator<HdfsFileMetadata> iterator = topicPartitionList.iterator();
        while (!batchSizeLimit.isOverLimit() && iterator.hasNext()) {
            HdfsFileMetadata r = iterator.next();
            // When going through the result, store the topic partition with the highest offset to the latestHdfsOffsetMap.
            if (latestHdfsOffsetMap.get(r.topicPartition) < r.endOffset) {
                latestHdfsOffsetMap.replace(r.topicPartition, r.endOffset);
                batchSizeLimit.add(r.hdfsFileSize);
            }
        }
        Map<String, Long> serializedHdfsOffset = new HashMap<>(latestHdfsOffsetMap.size());
        for (Map.Entry<TopicPartition, Long> entry : latestHdfsOffsetMap.entrySet()) {
            serializedHdfsOffset.put(entry.getKey().toString(), entry.getValue()); // offset
        }
        return new HdfsOffset(serializedHdfsOffset);
    }

    @Override
    public boolean isStub() {
        return stub;
    }
}
