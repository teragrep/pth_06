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

import com.teragrep.rlo_06.RFC5424Timestamp;

import java.time.Instant;
import java.util.Objects;

public final class EpochMicros {

    private final Instant instant;
    private final String source;
    private final long microsPerSecond;
    private final long nanosPerMicro;

    public EpochMicros(final RFC5424Timestamp rfc5424Timestamp) {
        this(rfc5424Timestamp.toZonedDateTime().toInstant(), "syslog");
    }

    public EpochMicros(final PathExtractedTimestamp pathExtractedTimestamp) {
        this(pathExtractedTimestamp.toZonedDateTime().toInstant(), "object-path");
    }

    public EpochMicros(final Instant instant, final String source) {
        this(instant, source, 1000L * 1000L, 1000L);
    }

    private EpochMicros(final Instant instant, final String source, long microsPerSecond, long nanosPerMicro) {
        this.instant = instant;
        this.source = source;
        this.microsPerSecond = microsPerSecond;
        this.nanosPerMicro = nanosPerMicro;
    }

    long asLong() {
        final long sec = Math.multiplyExact(instant.getEpochSecond(), microsPerSecond);
        return Math.addExact(sec, instant.getNano() / nanosPerMicro);
    }

    String source() {
        return source;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final EpochMicros that = (EpochMicros) object;
        return microsPerSecond == that.microsPerSecond && nanosPerMicro == that.nanosPerMicro
                && Objects.equals(instant, that.instant) && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instant, source, microsPerSecond, nanosPerMicro);
    }
}
