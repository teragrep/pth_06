package com.teragrep.pth06.planner;

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

import org.jooq.Record10;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.TreeMap;

import static com.teragrep.pth06.planner.MockDBData.generateResult;

// https://dzone.com/articles/easy-mocking-your-database-0

public class MockArchiveQueryProcessor implements ArchiveQuery {

    final Logger LOGGER = LoggerFactory.getLogger(MockArchiveQueryProcessor.class);

    private final String expectedQuery = "<index operation=\"EQUALS\" value=\"f17_v2\"/>";

    // epoch as key, resultSet as value
    private final TreeMap<Long, Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>>> virtualDatabaseMap;

    public MockArchiveQueryProcessor(String query) {

        if (!query.equals(expectedQuery)) {
            throw new IllegalArgumentException("query not expectedQuery: " + query);
        }

        MockDBData mockDBData = new MockDBData();

        this.virtualDatabaseMap = mockDBData.getVirtualDatabaseMap();

    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processInitialUnixEpochHour() {
        return virtualDatabaseMap.get(1262296800L);
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processAfterUnixEpochHour(long lastHour) {
        if (lastHour == 1262296800) {
            return virtualDatabaseMap.get(1262300400L);
        }
        if (lastHour == 1262300400) {
            return virtualDatabaseMap.get(1262383200L);
        }
        if (lastHour == 1262383200) {
            return virtualDatabaseMap.get(1262386800L);
        }
        if (lastHour == 1262386800) {
            return virtualDatabaseMap.get(1262469600L);
        }
        if (lastHour == 1262469600) {
            return virtualDatabaseMap.get(1262473200L);
        }
        if (lastHour == 1262473200) {
            return virtualDatabaseMap.get(1262556000L);
        }
        if (lastHour == 1262556000) {
            return virtualDatabaseMap.get(1262559600L);
        }
        if (lastHour == 1262559600) {
            return virtualDatabaseMap.get(1262642400L);
        }
        if (lastHour == 1262642400) {
            return virtualDatabaseMap.get(1262646000L);
        }
        if (lastHour == 1262646000) {
            return virtualDatabaseMap.get(1262728800L);
        }
        if (lastHour == 1262728800) {
            return virtualDatabaseMap.get(1262732400L);
        }
        if (lastHour == 1262732400) {
            return virtualDatabaseMap.get(1262815200L);
        }
        if (lastHour == 1262815200) {
            return virtualDatabaseMap.get(1262818800L);
        }
        if (lastHour == 1262818800) {
            return virtualDatabaseMap.get(1262901600L);
        }
        if (lastHour == 1262901600) {
            return virtualDatabaseMap.get(1262905200L);
        }
        if (lastHour == 1262905200) {
            return virtualDatabaseMap.get(1262988000L);
        }
        if (lastHour == 1262988000) {
            return virtualDatabaseMap.get(1262991600L);
        }
        if (lastHour == 1262991600) {
            return virtualDatabaseMap.get(1263074400L);
        }
        if (lastHour == 1263074400) {
            return virtualDatabaseMap.get(1263078000L);
        }
        if (lastHour == 1263078000) {
            return virtualDatabaseMap.get(1263160800L);
        }
        if (lastHour == 1263160800) {
            return virtualDatabaseMap.get(1263164400L);
        }
        if (lastHour == 1263164400) {
            return virtualDatabaseMap.get(1263247200L);
        }
        if (lastHour == 1263247200) {
            return virtualDatabaseMap.get(1263250800L);
        }
        if (lastHour == 1263250800) {
            return virtualDatabaseMap.get(1263333600L);
        }
        if (lastHour == 1263333600) {
            return virtualDatabaseMap.get(1263337200L);
        }
        if (lastHour == 1263337200) {
            return virtualDatabaseMap.get(1263420000L);
        }
        if (lastHour == 1263420000) {
            return virtualDatabaseMap.get(1263423600L);
        }
        if (lastHour == 1263423600) {
            return virtualDatabaseMap.get(1263506400L);
        }
        if (lastHour == 1263506400) {
            return virtualDatabaseMap.get(1263510000L);
        }
        if (lastHour == 1263510000) {
            return virtualDatabaseMap.get(1263592800L);
        }
        if (lastHour == 1263592800) {
            return virtualDatabaseMap.get(1263596400L);
        }
        if (lastHour == 1263596400) {
            return virtualDatabaseMap.get(1263679200L);
        }

        // empty set
        return generateResult(null, null, null, null, null, null, null, null, null, null);
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processBeforeUnixEpochHour(long lastHour) {
        LOGGER.debug("MockArchiveQueryProcessor.processBeforeUnixEpochHour> " + lastHour);

        Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> result = null;

        for (Long key : virtualDatabaseMap.keySet()) {
            if (key <= lastHour) {
                if (result == null) {
                    result = virtualDatabaseMap.get(key); 
                }
                else {
                    result.addAll(virtualDatabaseMap.get(key));
                }
                
            }
        }

        LOGGER.debug("MockArchiveQueryProcessor.processBeforeUnixEpochHour> returning " + result);
        if (result != null) {
            return result;
        }

        // empty set
        return generateResult(null, null, null, null, null, null, null, null, null, null);
    }

    @Override
    public Result<Record10<ULong, String, String, String, String, Date, String, String, ULong, ULong>> processBetweenUnixEpochHours(long startHour, long endHour) {
        // start is excluseive end is inclusive, however we do check both
        if (startHour == 1262296800 && endHour == 1262300400) {
            return virtualDatabaseMap.get(1262300400L);
        }
        if (startHour == 1262300400 && endHour == 1262383200) {
            return virtualDatabaseMap.get(1262383200L);
        }
        if (startHour == 1262383200 && endHour == 1262386800) {
            return virtualDatabaseMap.get(1262386800L);
        }
        if (startHour == 1262386800 && endHour == 1262469600) {
            return virtualDatabaseMap.get(1262469600L);
        }
        if (startHour == 1262469600 && endHour == 1262473200) {
            return virtualDatabaseMap.get(1262473200L);
        }
        if (startHour == 1262473200 && endHour == 1262556000) {
            return virtualDatabaseMap.get(1262556000L);
        }
        if (startHour == 1262556000 && endHour == 1262559600) {
            return virtualDatabaseMap.get(1262559600L);
        }
        if (startHour == 1262559600 && endHour == 1262642400) {
            return virtualDatabaseMap.get(1262642400L);
        }
        if (startHour == 1262642400 && endHour == 1262646000) {
            return virtualDatabaseMap.get(1262646000L);
        }
        if (startHour == 1262646000 && endHour == 1262728800) {
            return virtualDatabaseMap.get(1262728800L);
        }
        if (startHour == 1262728800 && endHour == 1262732400) {
            return virtualDatabaseMap.get(1262732400L);
        }
        if (startHour == 1262732400 && endHour == 1262815200) {
            return virtualDatabaseMap.get(1262815200L);
        }
        if (startHour == 1262815200 && endHour == 1262818800) {
            return virtualDatabaseMap.get(1262818800L);
        }
        if (startHour == 1262818800 && endHour == 1262901600) {
            return virtualDatabaseMap.get(1262901600L);
        }
        if (startHour == 1262901600 && endHour == 1262905200) {
            return virtualDatabaseMap.get(1262905200L);
        }
        if (startHour == 1262905200 && endHour == 1262988000) {
            return virtualDatabaseMap.get(1262988000L);
        }
        if (startHour == 1262988000 && endHour == 1262991600) {
            return virtualDatabaseMap.get(1262991600L);
        }
        if (startHour == 1262991600 && endHour == 1263074400) {
            return virtualDatabaseMap.get(1263074400L);
        }
        if (startHour == 1263074400 && endHour == 1263078000) {
            return virtualDatabaseMap.get(1263078000L);
        }
        if (startHour == 1263078000 && endHour == 1263160800) {
            return virtualDatabaseMap.get(1263160800L);
        }
        if (startHour == 1263160800 && endHour == 1263164400) {
            return virtualDatabaseMap.get(1263164400L);
        }
        if (startHour == 1263164400 && endHour == 1263247200) {
            return virtualDatabaseMap.get(1263247200L);
        }
        if (startHour == 1263247200 && endHour == 1263250800) {
            return virtualDatabaseMap.get(1263250800L);
        }
        if (startHour == 1263250800 && endHour == 1263333600) {
            return virtualDatabaseMap.get(1263333600L);
        }
        if (startHour == 1263333600 && endHour == 1263337200) {
            return virtualDatabaseMap.get(1263337200L);
        }
        if (startHour == 1263337200 && endHour == 1263420000) {
            return virtualDatabaseMap.get(1263420000L);
        }
        if (startHour == 1263420000 && endHour == 1263423600) {
            return virtualDatabaseMap.get(1263423600L);
        }
        if (startHour == 1263423600 && endHour == 1263506400) {
            return virtualDatabaseMap.get(1263506400L);
        }
        if (startHour == 1263506400 && endHour == 1263510000) {
            return virtualDatabaseMap.get(1263510000L);
        }
        if (startHour == 1263510000 && endHour == 1263592800) {
            return virtualDatabaseMap.get(1263592800L);
        }
        if (startHour == 1263592800 && endHour == 1263596400) {
            return virtualDatabaseMap.get(1263596400L);
        }
        if (startHour == 1263596400 && endHour == 1263679200) {
            return virtualDatabaseMap.get(1263679200L);
        }

        // empty set
        return generateResult(null, null, null, null, null, null, null, null, null, null);
    }

    @Override
    public void commit(long offset) {
        // end offset is committed
        virtualDatabaseMap.keySet().removeIf(hour -> hour <= offset);
    }
}
