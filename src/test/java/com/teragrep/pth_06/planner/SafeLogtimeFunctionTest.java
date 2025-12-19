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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class SafeLogtimeFunctionTest {

    private MariaDBContainer<?> mariadb;
    private Connection connection;
    private final ZoneId zoneId = ZoneId.of("Europe/Helsinki");

    @BeforeEach
    public void setup() {
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci", "--default-time-zone=" + zoneId.getId()));
        mariadb.start();
        connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
    }

    @AfterEach
    public void cleanup() {
        mariadb.stop();
        final boolean isClosed = Assertions.assertDoesNotThrow(connection::isClosed);
        if (!isClosed) {
            Assertions.assertDoesNotThrow(connection::close);
        }
    }

    @Test
    public void validPathTest() {
        final SafeLogtimeFunction fn = new SafeLogtimeFunction(DSL.field("path", String.class));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        final String validPath = "2010/01-08/sc-99-99-14-40/f17_v2/f17_v2.logGLOB-2010011601.log.gz";
        final Long result = ctx
                .select(fn.asField())
                .from(DSL.select(DSL.val(validPath).as("path")).asTable())
                .fetchOne(0, Long.class);

        final ZonedDateTime zdt = ZonedDateTime.of(2010, 1, 16, 1, 0, 0, 0, zoneId);
        long expectedEpoch = zdt.toEpochSecond();
        Assertions.assertEquals(expectedEpoch, result);
    }

    @Test
    public void invalidPathReturnsZeroEpochTest() {
        final SafeLogtimeFunction fn = new SafeLogtimeFunction(DSL.field("path", String.class));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        final Long result = ctx
                .select(fn.asField())
                .from(DSL.select(DSL.val("this/is/not/a/valid/path").as("path")).asTable())
                .fetchOne(0, Long.class);
        Assertions.assertEquals(0L, result);
    }

    @Test
    public void nullPathReturnsZeroEpochTest() {
        final SafeLogtimeFunction fn = new SafeLogtimeFunction(DSL.field("path", String.class));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        final Long result = ctx
                .select(fn.asField())
                .from(DSL.select(DSL.val((String) null).as("path")).asTable())
                .fetchOne(0, Long.class);
        Assertions.assertEquals(0L, result);
    }

    @Test
    public void malformedDateReturnsZeroEpochTest() {
        final SafeLogtimeFunction fn = new SafeLogtimeFunction(DSL.field("path", String.class));
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);
        final String invalidDatePath = "2010/01-16/folder/f17_v2/-@INVALIDDATE";
        final Long result = ctx
                .select(fn.asField())
                .from(DSL.select(DSL.val(invalidDatePath).as("path")).asTable())
                .fetchOne(0, Long.class);
        Assertions.assertEquals(0L, result);
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(SafeLogtimeFunction.class).verify();
    }
}
