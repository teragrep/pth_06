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

import com.teragrep.pth06.ArchiveS3ObjectMetadata;
import com.teragrep.pth06.Config;
import com.teragrep.pth06.scheduler.BatchSlice;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.jooq.Record;
import org.jooq.Record10;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.LinkedList;

/**
 * <h1>Archive Offset Planner</h1>
 *
 * Class for processing archive query offsets.
 *
 * @since 08/06/2022
 * @author Mikko Kortelainen
 *
 */
public class ArchiveOffsetPlanner implements OffsetPlanner {
    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveOffsetPlanner.class);

    // query utility
    private final ArchiveQuery aqp;

    // offset ranges
    private Long startEpochHour;
    private Long endEpochHour;

    public ArchiveOffsetPlanner(Config config) {
        this.aqp = new ArchiveQueryProcessor(config);
    }

    public ArchiveOffsetPlanner(ArchiveQuery queryProcessor) {
        this.aqp = queryProcessor;
    }

    @Override
    public LinkedList<BatchSlice> processInitial() {
        LOGGER.debug("processInitial()");
        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();
        // find start and the end
        startEpochHour = Long.MAX_VALUE;
        endEpochHour = Long.MIN_VALUE;

        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>>
                result = aqp.processInitialUnixEpochHour();

        for (Record r : result) {
            LOGGER.debug("processInitial() got record with hour " + r.get(8, ULong.class).longValue());
            long cur = r.get(8, ULong.class).longValue();
            if (cur < startEpochHour) {
                startEpochHour = cur;
                LOGGER.debug("processInitial> start update to " + cur);
            }
            if (cur > endEpochHour) {
                endEpochHour = cur;
                LOGGER.debug("processInitial> end update to " + cur);
            }

            taskObjectList.add(
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

        return taskObjectList;
    }

    @Override
    public LinkedList<BatchSlice> processBefore(Offset end) {
        LOGGER.debug("processBefore(): args " + end);
        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        // refresh end as it's set
        LongOffset eolo = (LongOffset) end;
        endEpochHour = eolo.offset();

        startEpochHour = Long.MAX_VALUE;

        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>>
                result = aqp.processBeforeUnixEpochHour(endEpochHour);
        for (Record r : result) {
            long cur = r.get(8, ULong.class).longValue();
            if (cur < startEpochHour) {
                startEpochHour = cur;
            }

            taskObjectList.add(
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
        // process all before id
        return taskObjectList;
    }

    @Override
    public LinkedList<BatchSlice> processAfter(Offset start) {
        LOGGER.debug("processAfter(): args " + start);
        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        // refresh start as it's set
        LongOffset solo = (LongOffset) start;
        startEpochHour = solo.offset();

        // set the new end
            Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>>
                    result = aqp.processAfterUnixEpochHour(startEpochHour);
            for (Record r : result) {
            long cur = r.get(8, ULong.class).longValue();
            if (cur > endEpochHour) {
                endEpochHour = cur;
            }

                taskObjectList.add(
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
        return taskObjectList;
    }

    @Override
    public LinkedList<BatchSlice> processRange(Offset start, Offset end) {
        LOGGER.debug("processRange(): args: start: " + start + " end: " + end);

        LinkedList<BatchSlice> taskObjectList = new LinkedList<>();

        // spark sends processRange for a retry

        // refresh start and end
        LongOffset solo = (LongOffset) start;
        startEpochHour = solo.offset();

        LongOffset eolo = (LongOffset) end;
        endEpochHour = eolo.offset();

        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>>
        result = aqp.processBetweenUnixEpochHours(startEpochHour, endEpochHour);

        for (Record r : result) {
            long cur = r.get(8, ULong.class).longValue();
            if (cur < startEpochHour) {
                startEpochHour = cur;
            }
            if (cur > endEpochHour) {
                endEpochHour = cur;
            }

            taskObjectList.add(
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
        return taskObjectList;
    }

    @Override
    public LongOffset getStartOffset() {
        LOGGER.debug("getStartOffset()");
        return new LongOffset(startEpochHour);
    }

    @Override
    public LongOffset getEndOffset() {
        LOGGER.debug("getEndOffset()");
        return new LongOffset(endEpochHour);
    }

    @Override
    public void commitOffsets(Offset offset) {
        LOGGER.debug("commitOffsets(): arg: offset: " + offset);
        LongOffset longOffset = (LongOffset) offset;
        aqp.commit(longOffset.offset());
    }

    @Override
    public Offset deserializeOffset(String s) {
        return new LongOffset(Long.parseLong(s));
    }
}
