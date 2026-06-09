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

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.records.LogfileRecord;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.*;
import java.time.*;
import java.util.HashMap;
import java.util.Map;

import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;

class EarliestConditionQueryTest {

    private MariaDBContainer<?> mariadb;
    private Connection connection;
    // Set the zoneId to system default
    private final ZoneId zoneId = ZoneId.systemDefault();
    private final String streamDBUsername = "streamdb";
    private final String streamDBPassword = "streamdb_pass";

    private final String streamdbName = "streamdb";
    private final String journaldbName = "journaldb";
    private final Map<String, String> opts = new HashMap<String, String>() {

        {
            put("S3endPoint", "mock");
            put("S3identity", "mock");
            put("S3credential", "mock");
            put("DBusername", streamDBUsername);
            put("DBpassword", streamDBPassword);
            put("DBstreamdbname", streamdbName);
            put("DBjournaldbname", journaldbName);
            put("queryXML", "<index value=\"example\" operation=\"EQUALS\"/>");
            put("archive.enabled", "true");
        }
    };

    @BeforeEach
    public void setup() {
        // Start mariadb testcontainer with timezone set to system default. Also creates a second streamdb database inside the container alongside the default journaldb.
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername(streamDBUsername).withPassword(streamDBPassword).withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci", "--default-time-zone=" + zoneId.getId()).withDatabaseName(journaldbName).withCopyFileToContainer(MountableFile.forClasspathResource("CREATE_STREAMDB_DB.sql"), "/docker-entrypoint-initdb.d/"));
        mariadb.start();
        connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
        // streamdb and journaldb is populated with test data during MariaDBContainer startup using CREATE_STREAMDB_DB.sql. Logfile table of journaldb is left empty for tests to populate it.
    }

    @AfterEach
    public void cleanup() {
        mariadb.stop();
    }

    private LogfileRecord logfileRecordForEpoch(long epoch, boolean hasNullEpochColumns) {
        Instant instant = Instant.ofEpochSecond(epoch);
        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        int year = zonedDateTime.getYear();
        // format 0 in front of string if 1-9
        String month = String.format("%02d", zonedDateTime.getMonthValue());
        String day = String.format("%02d", zonedDateTime.getDayOfMonth());
        String hour = String.format("%02d", zonedDateTime.getHour());

        String filename = "example.log-@" + epoch + "-" + year + month + day + hour + ".rfc5424.log.gz";
        String path = year + "/" + month + "-" + day + "/example.tg.dev.test/example/" + filename;
        LogfileRecord logfileRecord = new LogfileRecord(
                ULong.valueOf(epoch),
                Date.valueOf(zonedDateTime.toLocalDate()),
                Date.valueOf(zonedDateTime.plusYears(1).toLocalDate()),
                UShort.valueOf(1),
                path,
                null,
                UShort.valueOf(1),
                filename,
                new Timestamp(epoch),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "oldExample",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                ULong.valueOf(epoch),
                ULong.valueOf(epoch + (365 * 24 * 3600)),
                ULong.valueOf(epoch),
                null,
                ULong.valueOf(1),
                null
        );

        LogfileRecord nullEpochRecord = new LogfileRecord(
                ULong.valueOf(epoch),
                Date.valueOf(zonedDateTime.toLocalDate()),
                Date.valueOf(zonedDateTime.plusYears(1).toLocalDate()),
                UShort.valueOf(1),
                path,
                null,
                UShort.valueOf(1),
                filename,
                new Timestamp(epoch),
                ULong.valueOf(120L),
                "sha256 checksum 1",
                "archive tag 1",
                "oldExample",
                UShort.valueOf(2),
                UShort.valueOf(1),
                ULong.valueOf(390L),
                null,
                null,
                null,
                null,
                ULong.valueOf(1),
                null
        );

        if (hasNullEpochColumns) {
            return nullEpochRecord;
        }
        return logfileRecord;
    }

    @Test
    public void earliestConditionDefaultTimezoneQueryAllTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        ZonedDateTime instantPlusHour = instantZonedDateTime.plusHours(1);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();
        LogfileRecord logfileRecord2 = logfileRecordForEpoch(instantPlusHour.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        opts
                .put(
                        "queryXML",
                        "<AND><AND><index value=\"example\" operation=\"EQUALS\"/></AND><earliest value=\""
                                + instantZonedDateTime.toEpochSecond() + "\" operation=\"GE\"/></AND>"
                );
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // Pull the records from a specific logdate to the slicetable for further processing.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(2, rows);

                // find the earliest row and assert that it has correct offset/logtime value
                Assertions.assertFalse(sdc.getNextHourAndSizeFromSliceTable(0L).isStub);
                Assertions
                        .assertEquals(instantZonedDateTime.toEpochSecond(), sdc.getNextHourAndSizeFromSliceTable(0L).offset());
                // find the next row after earliest
                Assertions
                        .assertFalse(sdc.getNextHourAndSizeFromSliceTable(instantZonedDateTime.toEpochSecond()).isStub);
                Assertions
                        .assertEquals(
                                instantPlusHour.toEpochSecond(),
                                sdc.getNextHourAndSizeFromSliceTable(instantZonedDateTime.toEpochSecond()).offset()
                        );
            }
        });
    }

    @Test
    public void earliestConditionDefaultTimezoneQuerySingleTest() {
        // Add test data to logfile table in journaldb.
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        Instant instant = Instant.ofEpochSecond(1696471200L);
        ZonedDateTime instantZonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        ZonedDateTime instantPlusHour = instantZonedDateTime.plusHours(1);
        LogfileRecord logfileRecord = logfileRecordForEpoch(instantZonedDateTime.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord).execute();
        LogfileRecord logfileRecord2 = logfileRecordForEpoch(instantPlusHour.toEpochSecond(), true);
        ctx.insertInto(JOURNALDB.LOGFILE).set(logfileRecord2).execute();

        final Map<String, String> opts = this.opts;
        opts.put("DBurl", mariadb.getJdbcUrl());
        opts
                .put(
                        "queryXML",
                        "<AND><AND><index value=\"example\" operation=\"EQUALS\"/></AND><earliest value=\""
                                + instantPlusHour.toEpochSecond() + "\" operation=\"GE\"/></AND>"
                );
        final Config config = new Config(opts);
        Assertions.assertDoesNotThrow(() -> {
            try (final StreamDBClient sdc = new StreamDBClient(config)) {
                // Pull the records from a specific logdate to the slicetable for further processing.
                int rows = sdc.pullToSliceTable(Date.valueOf(instantZonedDateTime.toLocalDate()));
                Assertions.assertEquals(1, rows);

                // find the earliest row and assert that it has correct offset/logtime value
                Assertions.assertFalse(sdc.getNextHourAndSizeFromSliceTable(0L).isStub);
                Assertions
                        .assertEquals(instantPlusHour.toEpochSecond(), sdc.getNextHourAndSizeFromSliceTable(0L).offset());
            }
        });
    }

}
