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
package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.jooq.types.ULong;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;

public final class EpochDayCondition implements QueryCondition {

    private final Date date;

    public EpochDayCondition(final Date day) {
        this.date = day;
    }

    @Override
    public Condition condition() {
        final LocalDate localDate = date.toLocalDate();
        final long startEpoch = localDate.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        final long endEpoch = localDate.plusDays(1).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        if (startEpoch < 0 || endEpoch < 0) {
            throw new NumberFormatException("Date is before 1970-01-01 resulting in negative epoch value.");
        }
        return JOURNALDB.LOGFILE.EPOCH_HOUR.between(ULong.valueOf(startEpoch), ULong.valueOf(endEpoch));
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EpochDayCondition that = (EpochDayCondition) o;
        return Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(date);
    }
}
