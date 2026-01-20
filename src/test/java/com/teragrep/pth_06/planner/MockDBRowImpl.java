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
import java.util.Comparator;
import java.util.Objects;

public final class MockDBRowImpl implements MockDBRow {

    private final static Comparator<MockDBRow> COMPARATOR = Comparator
            .comparingLong(MockDBRow::logtime)
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

    private final long id;
    private final String directory;
    private final String stream;
    private final String host;
    private final String logtag;
    private final Date logdate;
    private final String bucket;
    private final String path;
    private final long logtime;
    private final long filesize;
    private final Long uncompressedFilesize;

    public MockDBRowImpl(
            final long id,
            final String directory,
            final String stream,
            final String host,
            final String logtag,
            final Date logdate,
            final String bucket,
            final String path,
            final long logtime,
            final long filesize,
            final Long uncompressedFilesize
    ) {
        this.id = id;
        this.directory = directory;
        this.stream = stream;
        this.host = host;
        this.logtag = logtag;
        this.logdate = logdate;
        this.bucket = bucket;
        this.path = path;
        this.logtime = logtime;
        this.filesize = filesize;
        this.uncompressedFilesize = uncompressedFilesize;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public String directory() {
        return directory;
    }

    @Override
    public String stream() {
        return stream;
    }

    @Override
    public String host() {
        return host;
    }

    @Override
    public String logtag() {
        return logtag;
    }

    @Override
    public Date logdate() {
        return logdate;
    }

    @Override
    public String bucket() {
        return bucket;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public long logtime() {
        return logtime;
    }

    @Override
    public long filesize() {
        return filesize;
    }

    @Override
    public Long uncompressedFilesize() {
        return uncompressedFilesize;
    }

    @Override
    public int compareTo(final MockDBRow o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MockDBRowImpl mockDBRow = (MockDBRowImpl) o;
        return id == mockDBRow.id
                && logtime == mockDBRow.logtime && filesize == mockDBRow.filesize && Objects
                        .equals(directory, mockDBRow.directory)
                && Objects.equals(stream, mockDBRow.stream) && Objects.equals(host, mockDBRow.host) && Objects.equals(logtag, mockDBRow.logtag) && Objects.equals(logdate, mockDBRow.logdate) && Objects.equals(bucket, mockDBRow.bucket) && Objects.equals(path, mockDBRow.path) && Objects.equals(uncompressedFilesize, mockDBRow.uncompressedFilesize);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(
                        id, directory, stream, host, logtag, logdate, bucket, path, logtime, filesize,
                        uncompressedFilesize
                );
    }

    @Override
    public String toString() {
        return "MockDBRowImpl{" + "id=" + id + ", directory='" + directory + '\'' + ", stream='" + stream + '\''
                + ", host='" + host + '\'' + ", logtag='" + logtag + '\'' + ", logdate=" + logdate + ", bucket='"
                + bucket + '\'' + ", path='" + path + '\'' + ", logtime=" + logtime + ", filesize=" + filesize
                + ", uncompressedFilesize=" + uncompressedFilesize + '}';
    }
}
