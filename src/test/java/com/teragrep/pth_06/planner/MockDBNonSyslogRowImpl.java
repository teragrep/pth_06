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

import org.jetbrains.annotations.NotNull;

import java.sql.Date;
import java.util.Comparator;
import java.util.Objects;

/**
 * Wrapper around MockDBRow where isSyslog() returns false
 */
public final class MockDBNonSyslogRowImpl implements MockDBRow {

    private final static Comparator<MockDBRow> COMPARATOR = Comparator
            .comparing(MockDBRow::isSyslog)
            .thenComparingLong(MockDBRow::logtime)
            .thenComparingLong(MockDBRow::id)
            .thenComparing(MockDBRow::directory)
            .thenComparing(MockDBRow::stream)
            .thenComparing(MockDBRow::host)
            .thenComparing(MockDBRow::logtag)
            .thenComparing(MockDBRow::logdate)
            .thenComparing(MockDBRow::bucket)
            .thenComparing(MockDBRow::path)
            .thenComparingLong(MockDBRow::filesize)
            .thenComparingLong(MockDBRow::uncompressedFilesize);
    private final boolean isSyslog;
    private final MockDBRow origin;

    public MockDBNonSyslogRowImpl(final MockDBRow origin) {
        this(origin, false);
    }

    private MockDBNonSyslogRowImpl(final MockDBRow origin, final boolean isSyslog) {
        this.origin = origin;
        this.isSyslog = isSyslog;
    }

    @Override
    public long id() {
        return origin.id();
    }

    @Override
    public String directory() {
        return origin.directory();
    }

    @Override
    public String stream() {
        return origin.stream();
    }

    @Override
    public String host() {
        return origin.host();
    }

    @Override
    public String logtag() {
        return origin.logtag();
    }

    @Override
    public Date logdate() {
        return origin.logdate();
    }

    @Override
    public String bucket() {
        return origin.bucket();
    }

    @Override
    public String path() {
        return origin.path();
    }

    @Override
    public long logtime() {
        return origin.logtime();
    }

    @Override
    public long filesize() {
        return origin.filesize();
    }

    @Override
    public Long uncompressedFilesize() {
        return origin.uncompressedFilesize();
    }

    @Override
    public boolean isSyslog() {
        return isSyslog;
    }

    @Override
    public int compareTo(@NotNull MockDBRow o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final MockDBNonSyslogRowImpl mockDBNonSyslogRow = (MockDBNonSyslogRowImpl) object;
        return Objects.equals(isSyslog, mockDBNonSyslogRow.isSyslog)
                && Objects.equals(origin, mockDBNonSyslogRow.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(origin, isSyslog);
    }

    @Override
    public String toString() {
        return "MockDBNonSyslogRowImpl{isSyslog=" + isSyslog + ", id=" + origin.id() + ", directory='"
                + origin.directory() + '\'' + ", stream='" + origin.stream() + '\'' + ", host='" + origin.host() + '\''
                + ", logtag='" + origin.logtag() + '\'' + ", logdate=" + origin.logdate() + ", bucket='"
                + origin.bucket() + '\'' + ", path='" + origin.path() + '\'' + ", logtime=" + origin.logtime()
                + ", filesize=" + origin.filesize() + ", uncompressedFilesize=" + origin.uncompressedFilesize() + '}';
    }
}
