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
package com.teragrep.pth_06.planner.walker;

import com.teragrep.pth_06.planner.walker.conditions.WithoutFiltersCondition;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 *
 * @see org.jooq.QueryPart
 */
public final class FilterlessSearchImplTest {

    private final String userName = "sa";
    private final String password = "";
    // matches IPv4
    private final String ipRegex = "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    private Connection conn;

    @BeforeEach
    public void setup() {
        final String url = "jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
        conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            final String filtertype = "CREATE TABLE`filtertype`" + "("
                    + "    `id`               bigint(20) unsigned   NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `expectedElements` bigint(20) unsigned NOT NULL,"
                    + "    `targetFpp`        DOUBLE(2) unsigned NOT NULL,"
                    + "    `pattern`          VARCHAR(2048) NOT NULL,"
                    + "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`))";
            final String ip = "CREATE TABLE `pattern_test`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            final String ip_2 = "CREATE TABLE `pattern_test_2`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(filtertype).execute();
            conn.prepareStatement(ip).execute();
            conn.prepareStatement(ip_2).execute();
            final String typeSQL = "INSERT INTO `filtertype` (`id`,`expectedElements`, `targetFpp`, `pattern`) VALUES (?,?,?,?)";
            for (int i = 1; i < 3; i++) {
                final PreparedStatement filterType = conn.prepareStatement(typeSQL);
                filterType.setInt(1, i);
                filterType.setInt(2, 1000 + i);
                filterType.setDouble(3, 0.01);
                filterType.setString(4, ipRegex);
                filterType.executeUpdate();
            }

            Assertions.assertDoesNotThrow(() -> writeFilter("pattern_test", 1));
            Assertions.assertDoesNotThrow(() -> writeFilter("pattern_test_2", 2));
        });
    }

    @AfterEach
    public void tearDown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testMatchCondition() {
        final FilterlessSearchImpl search = new FilterlessSearchImpl(DSL.using(conn), ipRegex);
        WithoutFiltersCondition condition = search.condition();
        Assertions.assertFalse(condition.requiredTables().isEmpty());
    }

    @Test
    public void testNoMatchCondition() {
        final FilterlessSearchImpl noMatchSearch = new FilterlessSearchImpl(DSL.using(conn), "nomatch-pattern");
        WithoutFiltersCondition condition = noMatchSearch.condition();
        Assertions.assertTrue(condition.requiredTables().isEmpty());
    }

    @Test
    public void testIsNotStub() {
        final FilterlessSearchImpl search = new FilterlessSearchImpl(DSL.using(conn), "pattern");
        Assertions.assertFalse(search.isStub());
    }

    private void writeFilter(final String tableName, final int filterId) throws SQLException {
        conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
        conn.prepareStatement("USE BLOOMDB").execute();
        final String sql = "INSERT INTO `" + tableName + "` (`partition_id`, `filter_type_id`, `filter`) "
                + "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
        final PreparedStatement stmt = conn.prepareStatement(sql);
        final BloomFilter filter = BloomFilter.create(1000, 0.01);
        final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
        Assertions.assertDoesNotThrow(() -> {
            filter.writeTo(filterBAOS);
            filterBAOS.close();
        });
        stmt.setInt(1, 1);
        stmt.setInt(2, filterId);
        stmt.setBytes(3, filterBAOS.toByteArray());
        stmt.executeUpdate();
        stmt.close();
    }
}
