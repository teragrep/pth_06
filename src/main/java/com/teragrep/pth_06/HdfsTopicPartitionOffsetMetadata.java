package com.teragrep.pth_06;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

// Class for holding serializable metadata of HDFS files containing kafka records.
// Maybe change the class name to something more appropriate. ie. HdfsFileMetadata
public class HdfsTopicPartitionOffsetMetadata implements Serializable {
    public final TopicPartition topicPartition; // Represents the Kafka topic partition which records the file contains.
    public final long endOffset; // Represents the offset of the record that was last added to the file.
    public final String hdfsFilePath; // Represents the file path where the file resides in HDFS.
    public final long hdfsFileSize; // Represents the size of the file in HDFS. Used for scheduling the batch slice.

    public HdfsTopicPartitionOffsetMetadata(TopicPartition topicPartition, long offset, String filePath, long fileSize)  {
        this.topicPartition = topicPartition;
        this.endOffset = offset;
        this.hdfsFilePath = filePath;
        this.hdfsFileSize = fileSize;
    }

    @Override
    public String toString() {
        return "HdfsTopicPartitionOffsetMetadata{" +
                "topicPartition=" + topicPartition +
                ", endOffset=" + endOffset +
                ", hdfsFilePath=" + hdfsFilePath +
                ", hdfsFileSize=" + hdfsFileSize +
                '}';
    }
}