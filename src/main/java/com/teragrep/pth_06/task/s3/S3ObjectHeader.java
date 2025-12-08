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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class S3ObjectHeader {

    private final String header;
    private final Pattern validPattern;

    S3ObjectHeader(final String header) {
        this(
                header,
                Pattern
                        .compile(
                                "^([12]\\d{3}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\\d|3[01])T(?:[01]\\d|2[0-3]):[0-5]\\d:(?:[0-5]\\d|60)(?:\\.\\d{1,6})?(?:Z|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)|-)$"
                        )
        );
    }

    private S3ObjectHeader(final String header, final Pattern validPattern) {
        this.header = header;
        this.validPattern = validPattern;
    }

    boolean isValid() {
        return validPattern.matcher(header).matches();
    }

    long epoch() {
        if (!isValid()) {
            throw new IllegalStateException("Cannot extract epoch for object not in syslog format");
        }
        Matcher matcher = validPattern.matcher(header);
        // timestamp expected in group 1, nil value checked
        if (!matcher.matches() || "-".equals(matcher.group(1))) {
            throw new IllegalStateException("Cannot extract a nil value from timestamp");
        }
        long epoch;
        try {
            final String timestampString = matcher.group(1);
            epoch = Instant.parse(timestampString).getEpochSecond();
        }
        catch (final DateTimeParseException e) {
            throw new IllegalArgumentException("RFC5424 format timestamp was not parseable");
        }
        return epoch;
    }
}
