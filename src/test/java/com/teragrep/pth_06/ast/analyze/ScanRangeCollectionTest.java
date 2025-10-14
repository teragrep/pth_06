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
package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.transform.WithDefaultValues;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class ScanRangeCollectionTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));

    @BeforeEach
    public void beforeEach() {
        // create streamdb tables
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS STREAMDB").execute();
            conn.prepareStatement("USE STREAMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS host").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS stream").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS log_group").execute();
            final String createHost = "CREATE TABLE `host` (" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                    + "  `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL,"
                    + "  `gid` int(10) unsigned NOT NULL," + "  PRIMARY KEY (`id`)," + "  KEY `host_gid` (`gid`),"
                    + "  KEY `idx_name_id` (`name`,`id`),"
                    + "  CONSTRAINT `host_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE"
                    + ")";
            final String createLogGroup = "CREATE TABLE `log_group` ("
                    + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                    + "  `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL," + "  PRIMARY KEY (`id`)" + ")";
            final String createStream = "CREATE TABLE `stream` (" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                    + "  `gid` int(10) unsigned NOT NULL,"
                    + "  `directory` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,"
                    + "  `stream` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,"
                    + "  `tag` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL," + "  PRIMARY KEY (`id`),"
                    + "  KEY `stream_gid` (`gid`),"
                    + "  CONSTRAINT `stream_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE"
                    + ")";
            conn.prepareStatement(createLogGroup).execute();
            conn.prepareStatement(createHost).execute();
            conn.prepareStatement(createStream).execute();
        });
    }

    @AfterAll
    public void stop() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testEmpty() {
        DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
        List<Expression> list = Arrays
                .asList(new XMLValueExpressionImpl("example", "EQUALS", Expression.Tag.INDEX), new XMLValueExpressionImpl("10", "EQUALS", Expression.Tag.EARLIEST), new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.LATEST));
        AndExpression andExpression = new AndExpression(list);
        ScanGroupExpression scanGroupExpression = new ScanGroupExpression(ctx, andExpression);
        List<ScanRange> scanRanges = scanGroupExpression.value();
        Assertions.assertTrue(scanRanges.isEmpty());
    }

    @Test
    public void testHost() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
        List<Expression> list = Arrays
                .asList(new XMLValueExpressionImpl("test_host", "EQUALS", Expression.Tag.HOST), new XMLValueExpressionImpl("10", "EQUALS", Expression.Tag.EARLIEST), new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.LATEST));
        Expression andExpression = new WithDefaultValues(24, new AndExpression(list)).transformed();
        ScanGroupExpression scanGroupExpression = new ScanGroupExpression(ctx, andExpression.asLogical());
        List<ScanRange> scanRanges = scanGroupExpression.value();
        Assertions.assertFalse(scanRanges.isEmpty());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(ScanRangeCollection.class)
                .withNonnullFields("config", "root", "scanRanges")
                .withIgnoredFields("LOGGER")
                .verify();
    }

    private void insertTestValues() throws SQLException {
        conn.prepareStatement("USE STREAMDB").execute();
        conn.prepareStatement("INSERT INTO `log_group` (`name`) VALUES ('test_group');").execute();
        conn.prepareStatement("INSERT INTO `log_group` (`name`) VALUES ('test_group_2');").execute();
        conn.prepareStatement("INSERT INTO `host` (`name`, `gid`) VALUES ('test_host', 1);").execute();
        conn.prepareStatement("INSERT INTO `host` (`name`, `gid`) VALUES ('test_host_2', 2);").execute();
        conn
                .prepareStatement(
                        "INSERT INTO `stream` (`gid`, `directory`, `stream`, `tag`) VALUES (1, 'test_directory', 'test_stream_1', 'test_tag');"
                )
                .execute();
        conn
                .prepareStatement(
                        "INSERT INTO `stream` (`gid`, `directory`, `stream`, `tag`) VALUES (2, 'test_directory_2', 'test_stream_2', 'test_tag');"
                )
                .execute();
    }
}
