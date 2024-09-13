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
package com.teragrep.pth_06.config;

import java.time.Instant;
import java.util.Map;

public final class HdfsConfig {

    // HDFS
    public final long cutoffOffset; // Represents the cutoff epoch which dictates which files should not be fetched from HDFS based on their timestamps.
    public final String hdfsPath; // Represents the working directory path in HDFS filesystem.
    public final String hdfsUri; // Represents the address of the HDFS server.
    public final boolean useMockHdfsDatabase; // Represents the configuration parameter if mock database should be used or not. Used only for testing.

    public final boolean isStub;

    public HdfsConfig(Map<String, String> opts) {
        cutoffOffset = Long
                .parseLong(
                        opts
                                .getOrDefault(
                                        "hdfs.hdfsCutoffOffset",
                                        String.valueOf(Instant.now().toEpochMilli() - 72 * 3600000)
                                )
                ); // Default is -72h from now
        hdfsPath = getOrThrow(opts, "hdfs.hdfsPath");
        hdfsUri = getOrThrow(opts, "hdfs.hdfsUri");
        String useMockHdfsDatabaseString = opts.getOrDefault("useMockHdfsDatabase", "false");
        useMockHdfsDatabase = "true".equals(useMockHdfsDatabaseString);
        isStub = false;
    }

    public HdfsConfig() {
        cutoffOffset = 0L;
        hdfsPath = "";
        hdfsUri = "";
        useMockHdfsDatabase = false;
        isStub = true;
    }

    private String getOrThrow(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Configuration item '" + key + "' was null!");
        }
        return value;
    }

}
