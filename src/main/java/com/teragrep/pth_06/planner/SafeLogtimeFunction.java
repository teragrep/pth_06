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

import org.jooq.Field;
import org.jooq.impl.DSL;

import java.util.Objects;

/**
 * Extracts a date string from a logfile path using a regex, converts it to a UNIX timestamp, and returns null if the
 * date cannot be extracted or is invalid.
 */
final class SafeLogtimeFunction {

    private final String regex;
    private final Field<String> pathField;

    SafeLogtimeFunction(final Field<String> pathField) {
        this(pathField, "^(?>.*-)([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})(?>\\\\..*)$");
    }

    private SafeLogtimeFunction(final Field<String> pathField, final String regex) {
        this.pathField = pathField;
        this.regex = regex;
    }

    /**
     * Extracts a timestamp from a logfile path and converts it to a UNIX epoch value.
     * <p>
     * Example path: 2010/01-08/sc-99-99-14-40/f17_v2/f17_v2.logGLOB-2010010801.log.gz
     * <p>
     * The regex captures the following groups from the filename portion after the last dash: 1 = YYYY, 2 = MM, 3 = DD,
     * 4 = HH
     *
     * @return a jOOQ Field representing the extracted UNIX epoch time, or null if the date cannot be extracted
     */
    Field<Long> asField() {
        // convert YYYYMMDD -> YYYY-MM-DDTHH
        final Field<String> extracted = DSL
                .field("REGEXP_REPLACE({0}, '" + regex + "', '\\\\1-\\\\2-\\\\3T\\\\4')", String.class, pathField);

        return DSL
                .when(extracted.likeRegex("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}$"), DSL.field("UNIX_TIMESTAMP(STR_TO_DATE({0}, '%Y-%m-%dT%H'))", Long.class, extracted)).otherwise(DSL.val(null, Long.class));
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final SafeLogtimeFunction that = (SafeLogtimeFunction) o;
        return Objects.equals(pathField, that.pathField) && Objects.equals(regex, that.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathField, regex);
    }
}
