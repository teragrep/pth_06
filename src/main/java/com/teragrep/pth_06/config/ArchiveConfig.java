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

import java.util.Map;

public final class ArchiveConfig {

    public final String dbUsername;
    public final String dbPassword;
    public final String dbUrl;
    public final String dbJournalDbName;
    public final String dbStreamDbName;

    // s3
    public final String s3EndPoint;
    public final String s3Identity;
    public final String s3Credential;

    public final boolean hideDatabaseExceptions;
    public final boolean skipNonRFC5424Files;

    public final long archiveIncludeBeforeEpoch;
    public final long defaultEarliestMinusHours;

    // hbase
    public final boolean isHBaseEnabled;

    // bloom
    public final boolean bloomEnabled;
    public final boolean withoutFilters;
    public final String withoutFiltersPattern;
    public final String bloomDbName;

    public final boolean isStub;

    public ArchiveConfig(Map<String, String> opts) {
        s3EndPoint = getOrThrow(opts, "S3endPoint");
        s3Identity = getOrThrow(opts, "S3identity");
        s3Credential = getOrThrow(opts, "S3credential");
        dbUsername = getOrThrow(opts, "DBusername");
        dbPassword = getOrThrow(opts, "DBpassword");
        dbUrl = getOrThrow(opts, "DBurl");
        isHBaseEnabled = opts.getOrDefault("hbase.enabled", "false").equalsIgnoreCase("true");
        bloomEnabled = opts.getOrDefault("bloom.enabled", "false").equalsIgnoreCase("true");
        withoutFilters = opts.getOrDefault("bloom.withoutFilters", "false").equalsIgnoreCase("true");
        withoutFiltersPattern = opts.getOrDefault("bloom.withoutFiltersPattern", "");
        bloomDbName = opts.getOrDefault("DBbloomdbname", "bloomdb");

        dbJournalDbName = opts.getOrDefault("DBjournaldbname", "journaldb");
        dbStreamDbName = opts.getOrDefault("DBstreamdbname", "streamdb");

        // hide exceptions
        hideDatabaseExceptions = opts.getOrDefault("hideDatabaseExceptions", "false").equalsIgnoreCase("true");

        // skip not rfc5424 parseable files
        skipNonRFC5424Files = opts.getOrDefault("skipNonRFC5424Files", "false").equalsIgnoreCase("true");

        archiveIncludeBeforeEpoch = Long
                .parseLong(opts.getOrDefault("archive.includeBeforeEpoch", String.valueOf(Long.MAX_VALUE)));

        defaultEarliestMinusHours = Long.parseLong(opts.getOrDefault("archive.defaultMinusHours", String.valueOf(24)));

        isStub = false;
    }

    public ArchiveConfig() {
        s3EndPoint = "";
        s3Identity = "";
        s3Credential = "";
        dbUsername = "";
        dbPassword = "";
        dbUrl = "";

        dbJournalDbName = "";
        dbStreamDbName = "";

        isHBaseEnabled = false;
        bloomEnabled = false;
        withoutFilters = false;
        withoutFiltersPattern = "";
        bloomDbName = "";

        hideDatabaseExceptions = false;
        skipNonRFC5424Files = false;

        archiveIncludeBeforeEpoch = 0L;
        defaultEarliestMinusHours = 24L;
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
