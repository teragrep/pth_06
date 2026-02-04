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
package com.teragrep.pth_06.task.s3;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class PathExtractedTimestamp {

    private final String path;
    private final ZoneId zoneId;
    private final Pattern hourPattern;
    private final Pattern datePattern;

    PathExtractedTimestamp(final String path) {
        this(path, ZoneId.of("Europe/Helsinki"));
    }

    PathExtractedTimestamp(final String path, final ZoneId zoneId) {
        this(
                path,
                zoneId,
                Pattern.compile("^(?>.*-)(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})(?<hour>\\d{2})(?>\\..*)$"),
                Pattern.compile("^(?<year>\\d{4})/(?<month>\\d{2})-(?<day>\\d{2})/(?>.*)$")
        );
    }

    PathExtractedTimestamp(
            final String path,
            final ZoneId zoneId,
            final Pattern hourPattern,
            final Pattern datePattern
    ) {
        this.path = path;
        this.zoneId = zoneId;
        this.hourPattern = hourPattern;
        this.datePattern = datePattern;
    }

    public ZonedDateTime toZonedDateTime() {
        final ZonedDateTime zonedDateTime;
        if (hasHourlyData()) {
            zonedDateTime = deriveFromHour();
        }
        else if (hasDateData()) {
            zonedDateTime = deriveFromDate();
        }
        else {
            throw new IllegalStateException("Path does not contain date information: <" + path + ">");
        }
        return zonedDateTime;
    }

    public boolean hasHourlyData() {
        return hourPattern.matcher(path).matches();
    }

    public boolean hasDateData() {
        return datePattern.matcher(path).matches();
    }

    private ZonedDateTime deriveFromHour() {
        final Matcher matcher = hourPattern.matcher(path);
        if (!matcher.matches()) {
            throw new IllegalStateException("Hour pattern did not match: <" + path + ">");
        }
        final int year = Integer.parseInt(matcher.group("year"));
        final int month = Integer.parseInt(matcher.group("month"));
        final int day = Integer.parseInt(matcher.group("day"));
        final int hour = Integer.parseInt(matcher.group("hour"));
        return ZonedDateTime.of(year, month, day, hour, 0, 0, 0, zoneId);
    }

    private ZonedDateTime deriveFromDate() {
        final Matcher matcher = datePattern.matcher(path);
        if (!matcher.matches()) {
            throw new IllegalStateException("Date pattern did not match: <" + path + ">");
        }
        final int year = Integer.parseInt(matcher.group("year"));
        final int month = Integer.parseInt(matcher.group("month"));
        final int day = Integer.parseInt(matcher.group("day"));
        return ZonedDateTime.of(year, month, day, 0, 0, 0, 0, zoneId);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final PathExtractedTimestamp that = (PathExtractedTimestamp) o;
        return Objects.equals(path, that.path) && Objects.equals(zoneId, that.zoneId)
                && Objects.equals(hourPattern, that.hourPattern) && Objects.equals(datePattern, that.datePattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, zoneId, hourPattern, datePattern);
    }
}
