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
import com.teragrep.pth06.Config;
import com.teragrep.pth06.planner.offset.DatasourceOffset;
import com.teragrep.pth06.planner.offset.KafkaOffset;
import com.teragrep.pth06.scheduler.BatchSlice;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;

/**
 * <h2>Combined Offset Planner</h2>
 *
 * Class that combines both Kafka and Archive offset planners. Initializes offset planners depending on the
 * settings set in the Config.
 *
 * @see ArchiveOffsetPlanner
 * @see KafkaOffsetPlanner
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class CombinedOffsetPlanner implements OffsetPlanner {
    private final Logger LOGGER = LoggerFactory.getLogger(CombinedOffsetPlanner.class);

    private final boolean isArchiveEnabled;
    private final boolean isKafkaEnabled;

    private final ArchiveOffsetPlanner archiveOffsetPlanner;
    private final KafkaOffsetPlanner kafkaOffsetPlanner;


    DatasourceOffset startOffset = null;
    DatasourceOffset endOffset = null;

    public CombinedOffsetPlanner(Config config) {
        this.isArchiveEnabled = config.isArchiveEnabled();
        this.isKafkaEnabled = config.isKafkaEnabled();

        if (this.isArchiveEnabled) {
            this.archiveOffsetPlanner = new ArchiveOffsetPlanner(config);
        }
        else {
            this.archiveOffsetPlanner = null;
        }
        if (this.isKafkaEnabled) {
            this.kafkaOffsetPlanner = new KafkaOffsetPlanner(config);
        }
        else {
            this.kafkaOffsetPlanner = null;
        }
    }

    @VisibleForTesting
    public CombinedOffsetPlanner(
            ArchiveQuery archiveQueryProcessor,
            KafkaQuery kafkaQueryProcessor
    ) {
        if (archiveQueryProcessor != null) {
            this.isArchiveEnabled = true;
            this.archiveOffsetPlanner = new ArchiveOffsetPlanner(archiveQueryProcessor);
        }
        else {
            this.isArchiveEnabled = false;
            this.archiveOffsetPlanner = null;
        }
        if (kafkaQueryProcessor != null) {
            this.isKafkaEnabled = true;
            this.kafkaOffsetPlanner = new KafkaOffsetPlanner(kafkaQueryProcessor);
        }
        else {
            this.isKafkaEnabled = false;
            this.kafkaOffsetPlanner = null;
        }
        LOGGER.debug("OffsetPlanner> testing instantiated");
    }

    @Override
    public LinkedList<BatchSlice> processInitial() {

        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        if (isArchiveEnabled) {
            taskObjectList.addAll(
                    archiveOffsetPlanner.processInitial()
            );
        }

        if (isKafkaEnabled) {
            taskObjectList.addAll(
                    kafkaOffsetPlanner.processInitial()
            );
        }

        updateOffsets();
        LOGGER.debug("processInitial(): rv: " + taskObjectList);
        return taskObjectList;
    }

    @Override
    public LinkedList<BatchSlice> processBefore(Offset end) {
        DatasourceOffset datasourceEndOffset = (DatasourceOffset) end;

        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        if (isArchiveEnabled) {
            LongOffset archiveEndOffset = datasourceEndOffset.getArchiveOffset();
            taskObjectList.addAll(
                    archiveOffsetPlanner.processBefore(
                            archiveEndOffset
                    )
            );
        }

        if (isKafkaEnabled) {
            KafkaOffset kafkaEndOffset = datasourceEndOffset.getKafkaOffset();
            taskObjectList.addAll(
                    kafkaOffsetPlanner.processBefore(
                            kafkaEndOffset
                    )
            );
        }

        updateOffsets();
        LOGGER.debug("processBefore(): args: " + end + " rv " + taskObjectList);
        return taskObjectList;
    }

    @Override
    public LinkedList<BatchSlice> processAfter(Offset start) {
        DatasourceOffset datasourceStartOffset = (DatasourceOffset) start;

        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        if (isArchiveEnabled) {
            LongOffset archiveStartOffset = datasourceStartOffset.getArchiveOffset();
            taskObjectList.addAll(
                    archiveOffsetPlanner.processAfter(
                            archiveStartOffset
                    )
            );
        }

        if (isKafkaEnabled) {
            KafkaOffset kafkaStartOffset = datasourceStartOffset.getKafkaOffset();
            taskObjectList.addAll(
                    kafkaOffsetPlanner.processAfter(
                            kafkaStartOffset
                    )
            );
        }


        updateOffsets();
        LOGGER.debug("processAfter(): args: " + start + " rv " + taskObjectList);
        return taskObjectList;
    }

    @Override
    public LinkedList<BatchSlice> processRange(Offset start, Offset end) {
        DatasourceOffset datasourceStartOffset = (DatasourceOffset) start;
        DatasourceOffset datasourceEndOffset = (DatasourceOffset) end;

        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        if (isArchiveEnabled) {
            LongOffset archiveStartOffset = datasourceStartOffset.getArchiveOffset();
            LongOffset archiveEndOffset = datasourceEndOffset.getArchiveOffset();

            taskObjectList.addAll(
                    archiveOffsetPlanner.processRange(
                            archiveStartOffset,
                            archiveEndOffset
                    )
            );
        }

        if (isKafkaEnabled) {
            KafkaOffset kafkaStartOffset = datasourceStartOffset.getKafkaOffset();
            KafkaOffset kafkaEndOffset = datasourceEndOffset.getKafkaOffset();

            taskObjectList.addAll(
                    kafkaOffsetPlanner.processRange(
                            kafkaStartOffset,
                            kafkaEndOffset
                    )
            );
        }

        updateOffsets();
        LOGGER.debug("processRange(): args: start " + start + " arg end: " + end + " rv: " + taskObjectList );
        return taskObjectList;
    }

    @Override
    public Offset getStartOffset() {
        return startOffset;
    }

    @Override
    public Offset getEndOffset() {
        return endOffset;
    }

    @Override
    public void commitOffsets(Offset offset) {
        LOGGER.debug("commitOffsets() arg: " + offset);
        DatasourceOffset datasourceOffset = (DatasourceOffset) offset;
        if (isArchiveEnabled) {
            LongOffset archiveOffset = datasourceOffset.getArchiveOffset();
            archiveOffsetPlanner.commitOffsets(archiveOffset);
        }
        if (isKafkaEnabled) {
            KafkaOffset kafkaOffset = datasourceOffset.getKafkaOffset();
            kafkaOffsetPlanner.commitOffsets(kafkaOffset);
        }
    }

    @Override
    public Offset deserializeOffset(String s) {
        return new DatasourceOffset(s);
    }

    private void updateOffsets() {
        LongOffset archiveStartOffset = null;
        LongOffset archiveEndOffset = null;

        if (isArchiveEnabled) {
            archiveStartOffset = archiveOffsetPlanner.getStartOffset();
            archiveEndOffset = archiveOffsetPlanner.getEndOffset();
        }

        KafkaOffset kafkaStartOffset = null;
        KafkaOffset kafkaEndOffset = null;

        if (isKafkaEnabled) {
            kafkaStartOffset = kafkaOffsetPlanner.getStartOffset();
            kafkaEndOffset = kafkaOffsetPlanner.getEndOffset();
        }

        startOffset = new DatasourceOffset(archiveStartOffset, kafkaStartOffset);
        endOffset = new DatasourceOffset(archiveEndOffset, kafkaEndOffset);

        LOGGER.debug("updateOffsets: start: " + startOffset
        + " end: " +endOffset);
    }
}
