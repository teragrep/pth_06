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

package com.teragrep.pth06.planner;

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.pth06.KafkaTopicPartitionOffsetMetadata;
import com.teragrep.pth06.planner.offset.KafkaOffset;
import com.teragrep.pth06.scheduler.BatchSlice;
import com.teragrep.pth06.Config;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

/**
 * <h2>Kafka Offset Planner</h2>
 *
 * Class for controlled execution of kafka query offsets.
 *
 * @see OffsetPlanner
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class KafkaOffsetPlanner implements OffsetPlanner{
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetPlanner.class);

    private final KafkaQuery kafkaQueryProcessor;

    private Map<TopicPartition, Long> startOffset;
    private Map<TopicPartition, Long> endOffset;

    private final boolean continuousProcesing;

    public KafkaOffsetPlanner(Config config) {
        this.kafkaQueryProcessor = new KafkaQueryProcessor(config);

        this.endOffset = kafkaQueryProcessor.getInitialEndOffsets();
        this.startOffset = kafkaQueryProcessor.getBeginningOffsets(new KafkaOffset(endOffset));

        this.continuousProcesing = config.getKafkaContinuousProcessing();
    }

    @VisibleForTesting
    public KafkaOffsetPlanner(KafkaQuery kafkaQueryProcessor) {
        this.kafkaQueryProcessor = kafkaQueryProcessor;

        this.endOffset = kafkaQueryProcessor.getInitialEndOffsets();
        this.startOffset = kafkaQueryProcessor.getBeginningOffsets(new KafkaOffset(endOffset));

        this.continuousProcesing = false;
    }


    @Override
    public LinkedList<BatchSlice> processInitial() {
        endOffset = kafkaQueryProcessor.getInitialEndOffsets();
        startOffset = kafkaQueryProcessor.getBeginningOffsets(new KafkaOffset(endOffset));
        LinkedList<BatchSlice> rv = generateTaskObjectList();
        LOGGER.debug("processInitial(): rv: " + rv );
        return rv;
    }

    @Override
    public LinkedList<BatchSlice> processBefore(Offset end) {
        KafkaOffset kafkaEndOffset = (KafkaOffset) end;
        startOffset = kafkaQueryProcessor.getBeginningOffsets(kafkaEndOffset);
        LinkedList<BatchSlice> rv = generateTaskObjectList();
        LOGGER.debug("processBefore(): arg " + end + " rv: " + rv );
        return rv;
    }

    @Override
    public LinkedList<BatchSlice> processAfter(Offset start) {
        LinkedList<BatchSlice> rv;
        if (continuousProcesing) {
            KafkaOffset kafkaStartOffset = (KafkaOffset) start;
            startOffset = kafkaStartOffset.getOffsetMap();
            endOffset = kafkaQueryProcessor.getEndOffsets(kafkaStartOffset);

            rv = generateTaskObjectList();

            LOGGER.debug("processAfter(): arg " + start + " rv: " + rv);

        }
        else {
            rv = new LinkedList<>();
        }
        return rv;
    }

    @Override
    public LinkedList<BatchSlice> processRange(Offset start, Offset end) {
        KafkaOffset kafkaStartOffset = (KafkaOffset) start;
        KafkaOffset kafkaEndOffset = (KafkaOffset) end;

        startOffset = kafkaStartOffset.getOffsetMap();
        endOffset = kafkaEndOffset.getOffsetMap();

        LinkedList<BatchSlice> rv = generateTaskObjectList();
        LOGGER.debug("processRange(): arg start " + start + " arg end: " + end + " rv: " + rv );
        return rv;    }

    @Override
    public KafkaOffset getStartOffset() {
        KafkaOffset rv = new KafkaOffset(startOffset);
        LOGGER.debug("getStartOffset(): rv: " + rv);
        return rv;
    }

    @Override
    public KafkaOffset getEndOffset() {
        KafkaOffset rv = new KafkaOffset(endOffset);
        LOGGER.debug("getEndOffset(): rv: " + rv);
        return rv;
    }

    @Override
    public void commitOffsets(Offset offset) {
        KafkaOffset kafkaOffset = (KafkaOffset) offset;
        kafkaQueryProcessor.commit(kafkaOffset);
    }

    @Override
    public Offset deserializeOffset(String s) {
        Offset rv = new KafkaOffset(s);
        LOGGER.debug("deserializeOffset(): arg: " + s + " rv: " + rv);
        return rv;
    }

    private LinkedList<BatchSlice> generateTaskObjectList() {
        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        for (Map.Entry<TopicPartition, Long> entry : startOffset.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long topicStart = entry.getValue();
            long topicEnd = endOffset.get(topicPartition);
            if (topicStart != topicEnd) {
                // new offsets available
                ArrayList<Long> offsetRangeList = new ArrayList<>();
                offsetRangeList.add(startOffset.get(entry.getKey())); // 0 start
                offsetRangeList.add(endOffset.get(entry.getKey())); // 1 end
                taskObjectList.add(
                        new BatchSlice(
                                new KafkaTopicPartitionOffsetMetadata(
                                        entry.getKey(),
                                        offsetRangeList
                                )
                        )
                );

            }
        }
        return taskObjectList;
    }
}
