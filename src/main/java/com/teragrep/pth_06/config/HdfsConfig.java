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
    public final long includeFileEpochAndAfter; // Represents the cutoff epoch in milliseconds which dictates which files should not be fetched from HDFS based on their timestamps.
    public final long includeRecordEpochAndAfter; // Represents the cutoff epoch in microseconds which dictates which record should not be fetched from HDFS based on the record timestamp.
    public final String hdfsPath; // Represents the working directory path in HDFS filesystem.
    public final String hdfsUri; // Represents the address of the HDFS server.
    public final String useHdfsHostname;
    public final String hdfsTransferProtection;
    public final String hdfsCipherSuites;
    public final boolean useMockHdfsDatabase; // Represents the configuration parameter if mock database should be used or not. Used only for testing.

    // Kerberos
    public final String kerberosAuthentication;
    public final String kerberosAuthorization;
    public final String kerberosPrincipalPattern;
    public final String kerberosKdc;
    public final String kerberosRealm;
    public final String kerberosKeytabUser;
    public final String kerberosKeytabPath;
    public final String kerberosAutorenewal;

    public final boolean isStub;

    public HdfsConfig(Map<String, String> opts) {
        includeFileEpochAndAfter = Long
                .parseLong(
                        opts
                                .getOrDefault(
                                        "hdfs.includeFileEpochAndAfter",
                                        String.valueOf(Instant.now().toEpochMilli() - 72 * 3600000)
                                )
                ); // Default is -72h from now
        includeRecordEpochAndAfter = Long
                .parseLong(
                        opts
                                .getOrDefault(
                                        "hdfs.includeRecordEpochAndAfter", String.valueOf(Long.MIN_VALUE / (1000 * 1000))
                                )
                );
        hdfsPath = getOrThrow(opts, "hdfs.hdfsPath");
        hdfsUri = getOrThrow(opts, "hdfs.hdfsUri");
        useHdfsHostname = getOrThrow(opts, "hdfs.useHostName");
        hdfsTransferProtection = getOrThrow(opts, "hdfs.transferProtection");
        hdfsCipherSuites = getOrThrow(opts, "hdfs.cipherSuites");
        String useMockHdfsDatabaseString = opts.getOrDefault("hdfs.useMockHdfsDatabase", "false");
        useMockHdfsDatabase = "true".equals(useMockHdfsDatabaseString);
        kerberosAuthentication = getOrThrow(opts, "hdfs.krbAuthentication");
        kerberosAuthorization = getOrThrow(opts, "hdfs.krbAuthorization");
        kerberosPrincipalPattern = getOrThrow(opts, "hdfs.krbPrincipalPattern");
        kerberosKdc = getOrThrow(opts, "hdfs.krbKdc");
        kerberosRealm = getOrThrow(opts, "hdfs.krbRealm");
        kerberosKeytabUser = getOrThrow(opts, "hdfs.krbKeytabUser");
        kerberosKeytabPath = getOrThrow(opts, "hdfs.krbKeytabPath");
        kerberosAutorenewal = opts.getOrDefault("hdfs.useKerberosAutorenewal", "false");
        isStub = false;
    }

    public HdfsConfig() {
        includeFileEpochAndAfter = 0L;
        includeRecordEpochAndAfter = 0L;
        hdfsPath = "";
        hdfsUri = "";
        useHdfsHostname = "";
        hdfsTransferProtection = "";
        hdfsCipherSuites = "";
        useMockHdfsDatabase = false;
        kerberosAuthentication = "";
        kerberosAuthorization = "";
        kerberosPrincipalPattern = "";
        kerberosKdc = "";
        kerberosRealm = "";
        kerberosKeytabUser = "";
        kerberosKeytabPath = "";
        kerberosAutorenewal = "false";
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
