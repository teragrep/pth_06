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

package com.teragrep.pth06;

import java.io.Serializable;

/**
 * <h1>Archive S3 Object Metadata</h1>
 *
 * Class for holding a serializable S3 archive object metadata.
 *
 * @since 17/01/2022
 * @author Mikko Kortelainen
 */
public class ArchiveS3ObjectMetadata implements Serializable {
	private final String id;
    private final String bucket;
    private final String path;
    private final String directory;
    private final String stream;
    private final String host;
    private final long logtimeEpoch;
    private final long compressedSize;

    public ArchiveS3ObjectMetadata(String id, String bucket, String path, String directory, String stream, String host, long logtimeEpoch, long compressedSize) {
        this.id = id;
		this.bucket = bucket;
        this.path = path;
        this.directory = directory;
        this.stream = stream;
        this.host = host;
        this.logtimeEpoch = logtimeEpoch;
        this.compressedSize = compressedSize;
    }

	public String getId() {
		return id;
	}

    public String getBucket() {
        return bucket;
    }

    public String getPath() {
        return path;
    }

    public String getDirectory() {
        return directory;
    }

    public String getStream() {
        return stream;
    }

    public String getHost() {
        return host;
    }

    public long getLogtimeEpoch() {
        return logtimeEpoch;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    @Override
    public String toString() {
        return "ArchiveS3ObjectMetadata{" +
                "bucket='" + bucket + '\'' +
                ", path='" + path + '\'' +
                ", directory='" + directory + '\'' +
                ", stream='" + stream + '\'' +
                ", host='" + host + '\'' +
                '}';
    }
}
