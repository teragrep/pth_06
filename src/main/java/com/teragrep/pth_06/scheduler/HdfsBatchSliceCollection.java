package com.teragrep.pth_06.scheduler;

import com.teragrep.pth_06.HdfsTopicPartitionOffsetMetadata;
import com.teragrep.pth_06.planner.HdfsQuery;
import com.teragrep.pth_06.planner.KafkaQuery;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import com.teragrep.pth_06.planner.offset.HdfsOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.connector.read.streaming.Offset;

import java.util.LinkedList;

public final class HdfsBatchSliceCollection extends BatchSliceCollection {
    private final Logger LOGGER = LoggerFactory.getLogger(HdfsBatchSliceCollection.class);
    private final HdfsQuery hq;

    public HdfsBatchSliceCollection(HdfsQuery hq)  {
        super();
        this.hq = hq;
    }

    public HdfsBatchSliceCollection processRange(Offset start, Offset end) {
        /*
        KAFKA:
        KafkaOffset kafkaStartOffset = ((DatasourceOffset)start).getKafkaOffset();
        KafkaOffset kafkaEndOffset = ((DatasourceOffset)end).getKafkaOffset();
        KafkaBatchSliceCollection rv = generate(kafkaStartOffset, kafkaEndOffset);
        LOGGER.debug("processRange(): arg start " + start + " arg end: " + end + " rv: " + rv );
        return rv;

        ARCHIVE:
        LOGGER.debug("processRange(): args: start: " + start + " end: " + end);
        this.clear(); // clear internal list
        Result<Record10<ULong, String, String, String, String, Date, String, String, Long, ULong>>
                result = aq.processBetweenUnixEpochHours(((DatasourceOffset)start).getArchiveOffset().offset(),
                                                        ((DatasourceOffset)end).getArchiveOffset().offset());
        for (Record r : result) {
            this.add(
                    new BatchSlice(
                            new ArchiveS3ObjectMetadata(
                                    r.get(0, String.class), // id
                                    r.get(6, String.class), // bucket
                                    r.get(7, String.class), // path
                                    r.get(1, String.class), // directory
                                    r.get(2, String.class), // stream
                                    r.get(3, String.class), // host
                                    r.get(8, Long.class), // logtime
                                    r.get(9, Long.class) // compressedSize
                            )
                    )
            );
        }
        return this;
        * */

        // HDFS:
        // If the slices are not distributed correctly, refactor the code to use the archive approach instead of kafka approach.
        LOGGER.debug("processRange(): args: start: " + start + " end: " + end);
        HdfsOffset hdfsStartOffset = ((DatasourceOffset)start).getHdfsOffset();
        HdfsOffset hdfsEndOffset = ((DatasourceOffset)end).getHdfsOffset();
        LinkedList<HdfsTopicPartitionOffsetMetadata> result = hq.processBetweenHdfsFileMetadata(hdfsStartOffset, hdfsEndOffset);
        for (HdfsTopicPartitionOffsetMetadata r : result) {
            this.add(
                    new BatchSlice(r)
            );
        }
        return this;
    }

}