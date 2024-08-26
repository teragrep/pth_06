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

import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.jooq.types.ULong;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.TreeMap;

public class MockDBData {

    private final TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> virtualDatabaseMap = new TreeMap<>();

    public MockDBData() {
        virtualDatabaseMap
                .put(
                        1262905200L,
                        generateResult(
                                "19181", "f17_v2", "log:f17_v2:0", "sc-99-99-14-40", "f17_v2", "2010-01-08",
                                "hundred-year", "2010/01-08/sc-99-99-14-40/f17_v2/f17_v2.logGLOB-2010010801.log.gz",
                                "1262905200", "28306039", "283060390"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263250800L,
                        generateResult(
                                "19183", "f17_v2", "log:f17_v2:0", "sc-99-99-14-140", "f17_v2", "2010-01-12",
                                "hundred-year", "2010/01-12/sc-99-99-14-140/f17_v2/f17_v2.logGLOB-2010011201.log.gz",
                                "1263250800", "28437606", "284376060"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262901600L,
                        generateResult(
                                "19213", "f17_v2", "log:f17_v2:0", "sc-99-99-14-43", "f17_v2", "2010-01-08",
                                "hundred-year", "2010/01-08/sc-99-99-14-43/f17_v2/f17_v2.logGLOB-2010010800.log.gz",
                                "1262901600", "28275031", "282750310"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263247200L,
                        generateResult(
                                "19235", "f17_v2", "log:f17_v2:0", "sc-99-99-14-139", "f17_v2", "2010-01-12",
                                "hundred-year", "2010/01-12/sc-99-99-14-139/f17_v2/f17_v2.logGLOB-2010011200.log.gz",
                                "1263247200", "28445548", "284455480"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262469600L,
                        generateResult(
                                "19323", "f17_v2", "log:f17_v2:0", "sc-99-99-14-228", "f17_v2", "2010-01-03",
                                "hundred-year", "2010/01-03/sc-99-99-14-228/f17_v2/f17_v2.logGLOB-2010010300.log.gz",
                                "1262469600", "28432476", "284324760"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263420000L,
                        generateResult(
                                "19409", "f17_v2", "log:f17_v2:0", "sc-99-99-14-5", "f17_v2", "2010-01-14",
                                "hundred-year", "2010/01-14/sc-99-99-14-5/f17_v2/f17_v2.logGLOB-2010011400.log.gz",
                                "1263420000", "28340118", "283401180"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262473200L,
                        generateResult(
                                "19423", "f17_v2", "log:f17_v2:0", "sc-99-99-14-131", "f17_v2", "2010-01-03",
                                "hundred-year", "2010/01-03/sc-99-99-14-131/f17_v2/f17_v2.logGLOB-2010010301.log.gz",
                                "1262473200", "28425247", "284252470"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263423600L,
                        generateResult(
                                "19475", "f17_v2", "log:f17_v2:0", "sc-99-99-14-66", "f17_v2", "2010-01-14",
                                "hundred-year", "2010/01-14/sc-99-99-14-66/f17_v2/f17_v2.logGLOB-2010011401.log.gz",
                                "1263423600", "28335303", "283353030"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263679200L,
                        generateResult(
                                "22039", "f17_v2", "log:f17_v2:0", "sc-99-99-14-26", "f17_v2", "2010-01-17",
                                "hundred-year", "2010/01-17/sc-99-99-14-26/f17_v2/f17_v2.logGLOB-2010011700.log.gz",
                                "1263679200", "28311253", "283112530"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262383200L,
                        generateResult(
                                "22221", "f17_v2", "log:f17_v2:0", "sc-99-99-14-33", "f17_v2", "2010-01-02",
                                "hundred-year", "2010/01-02/sc-99-99-14-33/f17_v2/f17_v2.logGLOB-2010010200.log.gz",
                                "1262383200", "28283307", "282833070"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262386800L,
                        generateResult(
                                "22243", "f17_v2", "log:f17_v2:0", "sc-99-99-14-254", "f17_v2", "2010-01-02",
                                "hundred-year", "2010/01-02/sc-99-99-14-254/f17_v2/f17_v2.logGLOB-2010010201.log.gz",
                                "1262386800", "28440136", "284401360"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262728800L,
                        generateResult(
                                "22259", "f17_v2", "log:f17_v2:0", "sc-99-99-14-201", "f17_v2", "2010-01-06",
                                "hundred-year", "2010/01-06/sc-99-99-14-201/f17_v2/f17_v2.logGLOB-2010010600.log.gz",
                                "1262728800", "28413194", "284131940"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262732400L,
                        generateResult(
                                "22279", "f17_v2", "log:f17_v2:0", "sc-99-99-14-252", "f17_v2", "2010-01-06",
                                "hundred-year", "2010/01-06/sc-99-99-14-252/f17_v2/f17_v2.logGLOB-2010010601.log.gz",
                                "1262732400", "28422986", "284229860"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263078000L,
                        generateResult(
                                "22503", "f17_v2", "log:f17_v2:0", "sc-99-99-14-233", "f17_v2", "2010-01-10",
                                "hundred-year", "2010/01-10/sc-99-99-14-233/f17_v2/f17_v2.logGLOB-2010011001.log.gz",
                                "1263078000", "28456873", "284568730"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263074400L,
                        generateResult(
                                "22557", "f17_v2", "log:f17_v2:0", "sc-99-99-14-163", "f17_v2", "2010-01-10",
                                "hundred-year", "2010/01-10/sc-99-99-14-163/f17_v2/f17_v2.logGLOB-2010011000.log.gz",
                                "1263074400", "28479632", "284796320"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263164400L,
                        generateResult(
                                "23481", "f17_v2", "log:f17_v2:0", "sc-99-99-14-229", "f17_v2", "2010-01-11",
                                "hundred-year", "2010/01-11/sc-99-99-14-229/f17_v2/f17_v2.logGLOB-2010011101.log.gz",
                                "1263164400", "28415774", "284157740"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263160800L,
                        generateResult(
                                "23495", "f17_v2", "log:f17_v2:0", "sc-99-99-14-193", "f17_v2", "2010-01-11",
                                "hundred-year", "2010/01-11/sc-99-99-14-193/f17_v2/f17_v2.logGLOB-2010011100.log.gz",
                                "1263160800", "28444834", "284448340"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262296800L,
                        generateResult(
                                "28653", "f17_v2", "log:f17_v2:0", "sc-99-99-14-148", "f17_v2", "2010-01-01",
                                "hundred-year", "2010/01-01/sc-99-99-14-148/f17_v2/f17_v2.logGLOB-2010010100.log.gz",
                                "1262296800", "28422002", "284220020"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262300400L,
                        generateResult(
                                "28703", "f17_v2", "log:f17_v2:0", "sc-99-99-14-84", "f17_v2", "2010-01-01",
                                "hundred-year", "2010/01-01/sc-99-99-14-84/f17_v2/f17_v2.logGLOB-2010010101.log.gz",
                                "1262300400", "28262884", "282628840"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262556000L,
                        generateResult(
                                "28873", "f17_v2", "log:f17_v2:0", "sc-99-99-14-226", "f17_v2", "2010-01-04",
                                "hundred-year", "2010/01-04/sc-99-99-14-226/f17_v2/f17_v2.logGLOB-2010010400.log.gz",
                                "1262556000", "28378705", "283787050"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262988000L,
                        generateResult(
                                "28897", "f17_v2", "log:f17_v2:0", "sc-99-99-14-4", "f17_v2", "2010-01-09",
                                "hundred-year", "2010/01-09/sc-99-99-14-4/f17_v2/f17_v2.logGLOB-2010010900.log.gz",
                                "1262988000", "28325370", "283253700"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262559600L,
                        generateResult(
                                "28911", "f17_v2", "log:f17_v2:0", "sc-99-99-14-112", "f17_v2", "2010-01-04",
                                "hundred-year", "2010/01-04/sc-99-99-14-112/f17_v2/f17_v2.logGLOB-2010010401.log.gz",
                                "1262559600", "28384499", "283844990"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262991600L,
                        generateResult(
                                "28979", "f17_v2", "log:f17_v2:0", "sc-99-99-14-20", "f17_v2", "2010-01-09",
                                "hundred-year", "2010/01-09/sc-99-99-14-20/f17_v2/f17_v2.logGLOB-2010010901.log.gz",
                                "1262991600", "28298372", "282983720"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263333600L,
                        generateResult(
                                "29031", "f17_v2", "log:f17_v2:0", "sc-99-99-14-210", "f17_v2", "2010-01-13",
                                "hundred-year", "2010/01-13/sc-99-99-14-210/f17_v2/f17_v2.logGLOB-2010011300.log.gz",
                                "1263333600", "28435159", "284351590"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263337200L,
                        generateResult(
                                "29047", "f17_v2", "log:f17_v2:0", "sc-99-99-14-35", "f17_v2", "2010-01-13",
                                "hundred-year", "2010/01-13/sc-99-99-14-35/f17_v2/f17_v2.logGLOB-2010011301.log.gz",
                                "1263337200", "28291081", "282910810"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262642400L,
                        generateResult(
                                "35379", "f17_v2", "log:f17_v2:0", "sc-99-99-14-18", "f17_v2", "2010-01-05",
                                "hundred-year", "2010/01-05/sc-99-99-14-18/f17_v2/f17_v2.logGLOB-2010010500.log.gz",
                                "1262642400", "28255532", "282555320"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262815200L,
                        generateResult(
                                "35439", "f17_v2", "log:f17_v2:0", "sc-99-99-14-23", "f17_v2", "2010-01-07",
                                "hundred-year", "2010/01-07/sc-99-99-14-23/f17_v2/f17_v2.logGLOB-2010010700.log.gz",
                                "1262815200", "28275870", "282758700"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262818800L,
                        generateResult(
                                "35443", "f17_v2", "log:f17_v2:0", "sc-99-99-14-105", "f17_v2", "2010-01-07",
                                "hundred-year", "2010/01-07/sc-99-99-14-105/f17_v2/f17_v2.logGLOB-2010010701.log.gz",
                                "1262818800", "28420114", "284201140"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263506400L,
                        generateResult(
                                "35453", "f17_v2", "log:f17_v2:0", "sc-99-99-14-5", "f17_v2", "2010-01-15",
                                "hundred-year", "2010/01-15/sc-99-99-14-5/f17_v2/f17_v2.logGLOB-2010011500.log.gz",
                                "1263506400", "28342297", "283422970"
                        )
                );
        virtualDatabaseMap
                .put(
                        1262646000L,
                        generateResult(
                                "35465", "f17_v2", "log:f17_v2:0", "sc-99-99-14-46", "f17_v2", "2010-01-05",
                                "hundred-year", "2010/01-05/sc-99-99-14-46/f17_v2/f17_v2.logGLOB-2010010501.log.gz",
                                "1262646000", "28258332", "282583320"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263510000L,
                        generateResult(
                                "35497", "f17_v2", "log:f17_v2:0", "sc-99-99-14-175", "f17_v2", "2010-01-15",
                                "hundred-year", "2010/01-15/sc-99-99-14-175/f17_v2/f17_v2.logGLOB-2010011501.log.gz",
                                "1263510000", "28453461", "284534610"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263592800L,
                        generateResult(
                                "35507", "f17_v2", "log:f17_v2:0", "sc-99-99-14-108", "f17_v2", "2010-01-16",
                                "hundred-year", "2010/01-16/sc-99-99-14-108/f17_v2/f17_v2.logGLOB-2010011600.log.gz",
                                "1263592800", "28464374", "284643740"
                        )
                );
        virtualDatabaseMap
                .put(
                        1263596400L,
                        generateResult(
                                "35525", "f17_v2", "log:f17_v2:0", "sc-99-99-14-125", "f17_v2", "2010-01-16",
                                "hundred-year", "2010/01-16/sc-99-99-14-125/f17_v2/f17_v2.logGLOB-2010011601.log.gz",
                                "1263596400", "28472164", "284721640"
                        )
                );
    }

    public TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> getVirtualDatabaseMap() {
        // id, directory, stream, host, logtag, logdate, bucket, path, logtime, filesize
        return virtualDatabaseMap;
    }

    static Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> generateResult(
            String id,
            String directory,
            String stream,
            String host,
            String logtag,
            String logdate,
            String bucket,
            String path,
            String logtime,
            String filesize,
            String uncompressedFilesize
    ) {
        MockDataProvider provider = new MockDataProvider() {

            @Override
            public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
                // use ordinary jooq api to create an org.jooq.result object.
                // you can also use ordinary jooq api to load csv files or
                // other formats, here!
                DSLContext create = DSL.using(SQLDialect.DEFAULT);
                Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result = create
                        .newResult(
                                StreamDBClient.SliceTable.id, StreamDBClient.SliceTable.directory,
                                StreamDBClient.SliceTable.stream, StreamDBClient.SliceTable.host,
                                StreamDBClient.SliceTable.logtag, StreamDBClient.SliceTable.logdate,
                                StreamDBClient.SliceTable.bucket, StreamDBClient.SliceTable.path,
                                StreamDBClient.SliceTable.logtime, StreamDBClient.SliceTable.filesize,
                                StreamDBClient.SliceTable.uncompressedFilesize
                        );
                //result.add(create.newRecord(1, "as", "asd", "as", "das", new Date(0), "sad", "path", 10, 10));

                Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> newRecord = create
                        .newRecord(
                                StreamDBClient.SliceTable.id, StreamDBClient.SliceTable.directory,
                                StreamDBClient.SliceTable.stream, StreamDBClient.SliceTable.host,
                                StreamDBClient.SliceTable.logtag, StreamDBClient.SliceTable.logdate,
                                StreamDBClient.SliceTable.bucket, StreamDBClient.SliceTable.path,
                                StreamDBClient.SliceTable.logtime, StreamDBClient.SliceTable.filesize,
                                StreamDBClient.SliceTable.uncompressedFilesize
                        );

                if (
                    id != null && directory != null && stream != null && host != null && logtag != null
                            && logdate != null && bucket != null && path != null && logtime != null && filesize != null
                ) {
                    newRecord.set(StreamDBClient.SliceTable.id, ULong.valueOf(id));
                    newRecord.set(StreamDBClient.SliceTable.directory, directory);
                    newRecord.set(StreamDBClient.SliceTable.stream, stream);
                    newRecord.set(StreamDBClient.SliceTable.host, host);
                    newRecord.set(StreamDBClient.SliceTable.logtag, logtag);
                    newRecord.set(StreamDBClient.SliceTable.logdate, Date.valueOf(logdate));
                    newRecord.set(StreamDBClient.SliceTable.bucket, bucket);
                    newRecord.set(StreamDBClient.SliceTable.path, path);
                    newRecord.set(StreamDBClient.SliceTable.logtime, Long.valueOf(logtime));
                    newRecord.set(StreamDBClient.SliceTable.filesize, ULong.valueOf(filesize));
                    newRecord.set(StreamDBClient.SliceTable.uncompressedFilesize, ULong.valueOf(uncompressedFilesize));

                    result.add(newRecord);
                } // else empty set

                // now, return 1-many results, depending on whether this is
                // a batch/multi-result context
                return new MockResult[] {
                        new MockResult(1, result)
                };
            }
        };

        Connection connection = new MockConnection(provider);
        DSLContext create = DSL.using(connection, SQLDialect.DEFAULT);

        return create
                .select(
                        StreamDBClient.SliceTable.id, StreamDBClient.SliceTable.directory,
                        StreamDBClient.SliceTable.stream, StreamDBClient.SliceTable.host,
                        StreamDBClient.SliceTable.logtag, StreamDBClient.SliceTable.logdate,
                        StreamDBClient.SliceTable.bucket, StreamDBClient.SliceTable.path,
                        StreamDBClient.SliceTable.logtime, StreamDBClient.SliceTable.filesize,
                        StreamDBClient.SliceTable.uncompressedFilesize
                )
                .fetch();

    }
}
