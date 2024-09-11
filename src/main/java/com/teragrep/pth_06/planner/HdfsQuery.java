package com.teragrep.pth_06.planner;

import com.google.gson.JsonArray;
import com.teragrep.pth_06.HdfsTopicPartitionOffsetMetadata;
import com.teragrep.pth_06.planner.offset.HdfsOffset;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.Map;

// TODO: Make an interface for for HDFS query. methods are placeholders at the moment.

// TODO: Fetches semi-recent data from HDFS.
public interface HdfsQuery {
    LinkedList<HdfsTopicPartitionOffsetMetadata> processBetweenHdfsFileMetadata(HdfsOffset startOffset, HdfsOffset endOffset); // TODO: PRESERVED IN REBASE, maybe rename it while at it.
    void commit(HdfsOffset offset); // TODO: PRESERVED IN REBASE
    JsonArray hdfsOffsetMapToJSON(); // TODO: PRESERVED IN REBASE!
    HdfsOffset getBeginningOffsets(); // TODO: NEW! This method returns the starting offsets for all available (aka. filtered) topic partitions.
    HdfsOffset getInitialEndOffsets(); // TODO: NEW! This method returns the end offsets for all available (aka. filtered) topic partitions.
}