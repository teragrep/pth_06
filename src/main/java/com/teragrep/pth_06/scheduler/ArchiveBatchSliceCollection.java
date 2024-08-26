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
package com.teragrep.pth_06.scheduler;

import com.teragrep.pth_06.ArchiveS3ObjectMetadata;
import com.teragrep.pth_06.planner.ArchiveQuery;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.jooq.Record;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;

public final class ArchiveBatchSliceCollection extends BatchSliceCollection {

    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveBatchSliceCollection.class);
    private final ArchiveQuery aq;

    public ArchiveBatchSliceCollection(ArchiveQuery aq) {
        super();
        this.aq = aq;
    }

    public ArchiveBatchSliceCollection processRange(Offset start, Offset end) {
        LOGGER.debug("processRange(): args: start: " + start + " end: " + end);

        this.clear(); // clear internal list

        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result = aq
                .processBetweenUnixEpochHours(
                        ((DatasourceOffset) start).getArchiveOffset().offset(),
                        ((DatasourceOffset) end).getArchiveOffset().offset()
                );

        for (Record r : result) {
            this
                    .add(new BatchSlice(new ArchiveS3ObjectMetadata(r.get(0, String.class), // id
                            r.get(6, String.class), // bucket
                            r.get(7, String.class), // path
                            r.get(1, String.class), // directory
                            r.get(2, String.class), // stream
                            r.get(3, String.class), // host
                            r.get(8, Long.class), // logtime
                            r.get(9, Long.class), // compressedSize
                            r.get(10, Long.class) // uncompressedSize
                    )));
        }
        return this;
    }
}
