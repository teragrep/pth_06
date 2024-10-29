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
package com.teragrep.pth_06.planner.bloomfilter;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

class SafeBatchTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));

    @BeforeEach
    void setup() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS target").execute();
            String targetTable = "CREATE TABLE `target`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE" + ")";
            conn.prepareStatement(targetTable).execute();
        });
    }

    @Test
    public void testOneInsert() {
        String sql = "INSERT INTO target (`partition_id`) VALUES(12345)";
        DSLContext ctx = DSL.using(conn);
        SafeBatch batch = new SafeBatch(ctx.batch(ctx.query(sql)));
        Assertions.assertDoesNotThrow(batch::execute);
        ResultSet result = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT `partition_id` FROM target").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int loops = 0;
            while (result.next()) {
                loops++;
                Assertions.assertEquals(12345L, result.getLong(1));
            }
            Assertions.assertEquals(1, loops);
        });
    }

    @Test
    public void testInsertTwo() {
        String sql1 = "INSERT INTO target (`partition_id`) VALUES(12345)";
        String sql2 = "INSERT INTO target (`partition_id`) VALUES(54321)";
        DSLContext ctx = DSL.using(conn);
        SafeBatch batch = new SafeBatch(ctx.batch(ctx.query(sql1), ctx.query(sql2)));
        Assertions.assertDoesNotThrow(batch::execute);
        ResultSet result = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT `partition_id` FROM target").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            List<Long> values = new ArrayList<>();
            int loops = 0;
            while (result.next()) {
                loops++;
                values.add(result.getLong(1));
            }
            Assertions.assertEquals(2, loops);
            Assertions.assertEquals(2, values.size());
            Assertions.assertEquals(12345L, values.get(0));
            Assertions.assertEquals(54321L, values.get(1));
        });
    }

    @Test
    public void testDataAccessException() {
        String sql1 = "INSERT INTO target (`partition_id`) VALUES(12345)";
        String sql2 = "INSERT INTO target (`partition_id`) VALUES(12345)";
        DSLContext ctx = DSL.using(conn);
        SafeBatch batch = new SafeBatch(ctx.batch(ctx.query(sql1), ctx.query(sql2)));
        Assertions.assertThrows(DataAccessException.class, batch::execute);
        ResultSet result = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT `partition_id` FROM target").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int loops = 0;
            while (result.next()) {
                Assertions.assertEquals(12345L, result.getLong(1));
                loops++;
            }
            Assertions.assertEquals(1, loops);
        });
    }
}
