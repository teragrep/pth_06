/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2024  Suomen Kanuuna Oy
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

package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.w3c.dom.Element;

import java.sql.Date;
import java.time.Instant;

import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;

public final class LatestCondition implements QueryCondition {
    private final Element element;

    public LatestCondition(Element element) {
        this.element = element;
    }

    public Condition condition() {
        final String value = element.getAttribute("value");
        // SQL connection uses localTime in the session, so we use unix to come over the conversions
        final Instant instant = Instant.ofEpochSecond(Integer.parseInt(value));
        final java.sql.Date timeQualifier = new Date(instant.toEpochMilli());
        Condition condition;
        condition = JOURNALDB.LOGFILE.LOGDATE.lessOrEqual(timeQualifier);
        condition = condition.and("UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H'))" +
                " <= " + instant.getEpochSecond());
        // raw SQL used here since following not supported for mariadb:
        // queryCondition = queryCondition.and(toTimestamp(
        // regexpReplaceAll(JOURNALDB.LOGFILE.PATH, "((^.*\\/.*-)|(\\.log\\.gz.*))", ""),
        // "YYYYMMDDHH24").lessOrEqual(Timestamp.from(instant)));
        // to match
        // 2021/09-27/sc-99-99-14-244/messages/messages-2021092722.gz.4
        // 2018/04-29/sc-99-99-14-245/f17/f17.logGLOB-2018042900.log.gz
        // NOTE uses literal path
        return condition;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;
        final LatestCondition cast = (LatestCondition) object;
        return this.condition().toString().equals(cast.condition().toString());
    }
}