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
package com.teragrep.pth_06.ast.meta;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StreamIDsTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    Connection conn;
    final Map<String, String> opts = new HashMap<>();

    @BeforeEach
    public void setup() {
        opts.put("queryXML", "query");
        opts.put("archive.enabled", "true");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", userName);
        opts.put("DBpassword", password);
        opts.put("DBurl", url);
        conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS STREAMDB").execute();
            conn.prepareStatement("USE STREAMDB").execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `log_group` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  PRIMARY KEY (`id`)\n" + ")"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `host` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `gid` int(10) unsigned NOT NULL,\n" + "  PRIMARY KEY (`id`),\n"
                                    + "  KEY `host_gid` (`gid`),\n" + "  KEY `idx_name_id` (`name`,`id`),\n"
                                    + "  CONSTRAINT `host_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE\n"
                                    + ")"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `stream` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `gid` int(10) unsigned NOT NULL,\n"
                                    + "  `directory` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `stream` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `tag` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  PRIMARY KEY (`id`),\n" + "  KEY `stream_gid` (`gid`),\n"
                                    + "  CONSTRAINT `stream_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE\n"
                                    + ") "
                    )
                    .execute();
        });
    }

    @AfterEach
    public void stop() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testSingleIndex() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        XMLValueExpressionImpl indexExpression = new XMLValueExpressionImpl(
                "test_directory",
                "EQUALS",
                Expression.Tag.INDEX
        );
        Condition streamdbCondition = new StreamDBCondition(
                indexExpression,
                Collections.emptyList(),
                Collections.emptyList()
        ).condition();
        StreamIDs streamIDs = new StreamIDs(DSL.using(conn), streamdbCondition);
        List<Long> streamIdList = streamIDs.streamIdList();
        List<Long> expectedList = Collections.singletonList(1L);
        Assertions.assertEquals(expectedList, streamIdList);
    }

    @Test
    public void testMultipleIndex() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        XMLValueExpressionImpl indexExpression = new XMLValueExpressionImpl("*", "EQUALS", Expression.Tag.INDEX);
        Condition streamdbCondition = new StreamDBCondition(
                indexExpression,
                Collections.emptyList(),
                Collections.emptyList()
        ).condition();
        StreamIDs streamIDs = new StreamIDs(DSL.using(conn), streamdbCondition);
        List<Long> streamIdList = streamIDs.streamIdList();
        List<Long> expectedList = Arrays.asList(1L, 2L);
        Assertions.assertEquals(expectedList, streamIdList);
    }

    @Test
    public void testSingleHost() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        XMLValueExpressionImpl indexExpression = new XMLValueExpressionImpl("*", "EQUALS", Expression.Tag.INDEX);
        XMLValueExpressionImpl hostCondition = new XMLValueExpressionImpl("test_host_2", "EQUALS", Expression.Tag.HOST);
        Condition streamdbCondition = new StreamDBCondition(
                indexExpression,
                Collections.singletonList(hostCondition),
                Collections.emptyList()
        ).condition();
        StreamIDs streamIDs = new StreamIDs(DSL.using(conn), streamdbCondition);
        List<Long> streamIdList = streamIDs.streamIdList();
        List<Long> expectedList = Collections.singletonList(2L);
        Assertions.assertEquals(expectedList, streamIdList);
    }

    @Test
    public void testMultipleHosts() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        XMLValueExpressionImpl indexExpression = new XMLValueExpressionImpl("*", "EQUALS", Expression.Tag.INDEX);
        XMLValueExpressionImpl hostCondition = new XMLValueExpressionImpl("test*", "EQUALS", Expression.Tag.HOST);
        Condition streamdbCondition = new StreamDBCondition(
                indexExpression,
                Collections.singletonList(hostCondition),
                Collections.emptyList()
        ).condition();
        StreamIDs streamIDs = new StreamIDs(DSL.using(conn), streamdbCondition);
        List<Long> streamIdList = streamIDs.streamIdList();
        List<Long> expectedList = Arrays.asList(1L, 2L);
        Assertions.assertEquals(expectedList, streamIdList);
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(StreamIDs.class)
                .withNonnullFields("ctx", "condition")
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
