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

import java.sql.Date;
import java.util.PriorityQueue;

public final class MockDBRowSource implements TestDataSource {

    @Override
    public PriorityQueue<MockDBRow> asPriorityQueue() {
        final PriorityQueue<MockDBRow> mockDBRows = new PriorityQueue<>();
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19181L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-40",
                                "f17_v2",
                                Date.valueOf("2010-01-08"),
                                "hundred-year",
                                "2010/01-08/sc-99-99-14-40/f17_v2/f17_v2.logGLOB-2010010801.log.gz",
                                1262905200L,
                                28306039L,
                                null
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19183L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-140",
                                "f17_v2",
                                Date.valueOf("2010-01-12"),
                                "hundred-year",
                                "2010/01-12/sc-99-99-14-140/f17_v2/f17_v2.logGLOB-2010011201.log.gz",
                                1263250800L,
                                28437606L,
                                284376060L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19213L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-43",
                                "f17_v2",
                                Date.valueOf("2010-01-08"),
                                "hundred-year",
                                "2010/01-08/sc-99-99-14-43/f17_v2/f17_v2.logGLOB-2010010800.log.gz",
                                1262901600L,
                                28275031L,
                                282750310L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19235L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-139",
                                "f17_v2",
                                Date.valueOf("2010-01-12"),
                                "hundred-year",
                                "2010/01-12/sc-99-99-14-139/f17_v2/f17_v2.logGLOB-2010011200.log.gz",
                                1263247200L,
                                28445548L,
                                284455480L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19323L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-228",
                                "f17_v2",
                                Date.valueOf("2010-01-03"),
                                "hundred-year",
                                "2010/01-03/sc-99-99-14-228/f17_v2/f17_v2.logGLOB-2010010300.log.gz",
                                1262469600L,
                                28432476L,
                                284324760L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19409L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-5",
                                "f17_v2",
                                Date.valueOf("2010-01-14"),
                                "hundred-year",
                                "2010/01-14/sc-99-99-14-5/f17_v2/f17_v2.logGLOB-2010011400.log.gz",
                                1263420000L,
                                28340118L,
                                283401180L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19423L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-131",
                                "f17_v2",
                                Date.valueOf("2010-01-03"),
                                "hundred-year",
                                "2010/01-03/sc-99-99-14-131/f17_v2/f17_v2.logGLOB-2010010301.log.gz",
                                1262473200L,
                                28425247L,
                                284252470L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                19475L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-66",
                                "f17_v2",
                                Date.valueOf("2010-01-14"),
                                "hundred-year",
                                "2010/01-14/sc-99-99-14-66/f17_v2/f17_v2.logGLOB-2010011401.log.gz",
                                1263423600L,
                                28335303L,
                                283353030L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22039L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-26",
                                "f17_v2",
                                Date.valueOf("2010-01-17"),
                                "hundred-year",
                                "2010/01-17/sc-99-99-14-26/f17_v2/f17_v2.logGLOB-2010011700.log.gz",
                                1263679200L,
                                28311253L,
                                283112530L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22221L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-33",
                                "f17_v2",
                                Date.valueOf("2010-01-02"),
                                "hundred-year",
                                "2010/01-02/sc-99-99-14-33/f17_v2/f17_v2.logGLOB-2010010200.log.gz",
                                1262383200L,
                                28283307L,
                                282833070L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22243L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-254",
                                "f17_v2",
                                Date.valueOf("2010-01-02"),
                                "hundred-year",
                                "2010/01-02/sc-99-99-14-254/f17_v2/f17_v2.logGLOB-2010010201.log.gz",
                                1262386800L,
                                28440136L,
                                284401360L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22259L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-201",
                                "f17_v2",
                                Date.valueOf("2010-01-06"),
                                "hundred-year",
                                "2010/01-06/sc-99-99-14-201/f17_v2/f17_v2.logGLOB-2010010600.log.gz",
                                1262728800L,
                                28413194L,
                                284131940L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22279L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-252",
                                "f17_v2",
                                Date.valueOf("2010-01-06"),
                                "hundred-year",
                                "2010/01-06/sc-99-99-14-252/f17_v2/f17_v2.logGLOB-2010010601.log.gz",
                                1262732400L,
                                28422986L,
                                284229860L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22503L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-233",
                                "f17_v2",
                                Date.valueOf("2010-01-10"),
                                "hundred-year",
                                "2010/01-10/sc-99-99-14-233/f17_v2/f17_v2.logGLOB-2010011001.log.gz",
                                1263078000L,
                                28456873L,
                                284568730L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                22557L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-163",
                                "f17_v2",
                                Date.valueOf("2010-01-10"),
                                "hundred-year",
                                "2010/01-10/sc-99-99-14-163/f17_v2/f17_v2.logGLOB-2010011000.log.gz",
                                1263074400L,
                                28479632L,
                                284796320L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                23481L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-229",
                                "f17_v2",
                                Date.valueOf("2010-01-11"),
                                "hundred-year",
                                "2010/01-11/sc-99-99-14-229/f17_v2/f17_v2.logGLOB-2010011101.log.gz",
                                1263164400L,
                                28415774L,
                                284157740L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                23495L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-193",
                                "f17_v2",
                                Date.valueOf("2010-01-11"),
                                "hundred-year",
                                "2010/01-11/sc-99-99-14-193/f17_v2/f17_v2.logGLOB-2010011100.log.gz",
                                1263160800L,
                                28444834L,
                                284448340L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                28653L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-148",
                                "f17_v2",
                                Date.valueOf("2010-01-01"),
                                "hundred-year",
                                "2010/01-01/sc-99-99-14-148/f17_v2/f17_v2.logGLOB-2010010100.log.gz",
                                1262296800L,
                                28422002L,
                                284220020L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                28703L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-84",
                                "f17_v2",
                                Date.valueOf("2010-01-01"),
                                "hundred-year",
                                "2010/01-01/sc-99-99-14-84/f17_v2/f17_v2.logGLOB-2010010101.log.gz",
                                1262300400L,
                                28262884L,
                                282628840L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                28873L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-226",
                                "f17_v2",
                                Date.valueOf("2010-01-04"),
                                "hundred-year",
                                "2010/01-04/sc-99-99-14-226/f17_v2/f17_v2.logGLOB-2010010400.log.gz",
                                1262556000L,
                                28378705L,
                                283787050L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                28897L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-4",
                                "f17_v2",
                                Date.valueOf("2010-01-09"),
                                "hundred-year",
                                "2010/01-09/sc-99-99-14-4/f17_v2/f17_v2.logGLOB-2010010900.log.gz",
                                1262988000L,
                                28325370L,
                                283253700L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                28911L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-112",
                                "f17_v2",
                                Date.valueOf("2010-01-04"),
                                "hundred-year",
                                "2010/01-04/sc-99-99-14-112/f17_v2/f17_v2.logGLOB-2010010401.log.gz",
                                1262559600L,
                                28384499L,
                                283844990L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                28979L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-20",
                                "f17_v2",
                                Date.valueOf("2010-01-09"),
                                "hundred-year",
                                "2010/01-09/sc-99-99-14-20/f17_v2/f17_v2.logGLOB-2010010901.log.gz",
                                1262991600L,
                                28298372L,
                                282983720L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                29031L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-210",
                                "f17_v2",
                                Date.valueOf("2010-01-13"),
                                "hundred-year",
                                "2010/01-13/sc-99-99-14-210/f17_v2/f17_v2.logGLOB-2010011300.log.gz",
                                1263333600L,
                                28435159L,
                                284351590L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                29047L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-35",
                                "f17_v2",
                                Date.valueOf("2010-01-13"),
                                "hundred-year",
                                "2010/01-13/sc-99-99-14-35/f17_v2/f17_v2.logGLOB-2010011301.log.gz",
                                1263337200L,
                                28291081L,
                                282910810L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35379L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-18",
                                "f17_v2",
                                Date.valueOf("2010-01-05"),
                                "hundred-year",
                                "2010/01-05/sc-99-99-14-18/f17_v2/f17_v2.logGLOB-2010010500.log.gz",
                                1262642400L,
                                28255532L,
                                282555320L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35439L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-23",
                                "f17_v2",
                                Date.valueOf("2010-01-07"),
                                "hundred-year",
                                "2010/01-07/sc-99-99-14-23/f17_v2/f17_v2.logGLOB-2010010700.log.gz",
                                1262815200L,
                                28275870L,
                                282758700L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35443L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-105",
                                "f17_v2",
                                Date.valueOf("2010-01-07"),
                                "hundred-year",
                                "2010/01-07/sc-99-99-14-105/f17_v2/f17_v2.logGLOB-2010010701.log.gz",
                                1262818800L,
                                28420114L,
                                284201140L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35453L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-5",
                                "f17_v2",
                                Date.valueOf("2010-01-15"),
                                "hundred-year",
                                "2010/01-15/sc-99-99-14-5/f17_v2/f17_v2.logGLOB-2010011500.log.gz",
                                1263506400L,
                                28342297L,
                                283422970L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35465L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-46",
                                "f17_v2",
                                Date.valueOf("2010-01-05"),
                                "hundred-year",
                                "2010/01-05/sc-99-99-14-46/f17_v2/f17_v2.logGLOB-2010010501.log.gz",
                                1262646000L,
                                28258332L,
                                282583320L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35497L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-175",
                                "f17_v2",
                                Date.valueOf("2010-01-15"),
                                "hundred-year",
                                "2010/01-15/sc-99-99-14-175/f17_v2/f17_v2.logGLOB-2010011501.log.gz",
                                1263510000L,
                                28453461L,
                                284534610L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35507L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-108",
                                "f17_v2",
                                Date.valueOf("2010-01-16"),
                                "hundred-year",
                                "2010/01-16/sc-99-99-14-108/f17_v2/f17_v2.logGLOB-2010011600.log.gz",
                                1263592800L,
                                28464374L,
                                284643740L
                        )
                );
        mockDBRows
                .add(

                        new MockDBRowImpl(
                                35525L,
                                "f17_v2",
                                "log:f17_v2:0",
                                "sc-99-99-14-125",
                                "f17_v2",
                                Date.valueOf("2010-01-16"),
                                "hundred-year",
                                "2010/01-16/sc-99-99-14-125/f17_v2/f17_v2.logGLOB-2010011601.log.gz",
                                1263596400L,
                                28472164L,
                                284721640L
                        )
                );

        return new PriorityQueue<>(mockDBRows);
    }
}
