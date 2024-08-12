/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022, 2023, 2024  Suomen Kanuuna Oy
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

package com.teragrep.pth_06.planner;

import com.teragrep.blf_01.Token;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BloomFilterTempTableTest {
    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    List<String> patternList = new ArrayList<>(Arrays.asList(
            "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}",
            "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}"
    ));
    Connection conn;
    DSLContext ctx;

    @BeforeAll
    public void setup() {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
        Assertions.assertDoesNotThrow(() -> {
            Connection conn = DriverManager.getConnection(url, userName, password);
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS filtertype").execute();
            String filtertype = "CREATE TABLE`filtertype`" +
                    "(" +
                    "    `id`               bigint(20) unsigned   NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `expectedElements` bigint(20) unsigned NOT NULL," +
                    "    `targetFpp`        DOUBLE(2) unsigned NOT NULL," +
                    "    `pattern`          VARCHAR(2048) NOT NULL," +
                    "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`)" +
                    ")";
            conn.prepareStatement(filtertype).execute();
            String typeSQL = "INSERT INTO `filtertype` (`id`,`expectedElements`, `targetFpp`, `pattern`) VALUES (?,?,?,?)";
            int id = 1;
            for (String pattern : patternList) {
                PreparedStatement filterType = conn.prepareStatement(typeSQL);
                filterType.setInt(1, id);
                filterType.setInt(2, 1000);
                filterType.setDouble(3, 0.01);
                filterType.setString(4, pattern);
                filterType.executeUpdate();
                id++;
            }
            this.conn = conn;
            this.ctx = DSL.using(conn);
        });
    }

    @BeforeEach
    void createTargetTable() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP TABLE IF EXISTS target").execute();
            String targetTable = "CREATE TABLE `target`(" +
                    "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                    "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE," +
                    "    `filter_type_id` bigint(20) unsigned NOT NULL," +
                    "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(targetTable).execute();
        });
    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP ALL OBJECTS").execute(); //h2 clear database
            conn.close();
        });
    }

    @Test
    public void testCreate() {
        Assertions.assertDoesNotThrow(() -> {
            Table<?> table = ctx.meta()
                    .filterSchemas(s -> s.getName().equals("bloomdb"))
                    .filterTables(t -> !t.getName().equals("filtertype"))
                    .getTables()
                    .get(0);
            Set<Token> tokenSet = new PatternMatch(ctx, "test").tokenSet();
            BloomFilterTempTable tempTable = new BloomFilterTempTable(ctx, table, 0L, tokenSet);
            tempTable.create();
        });
    }

    @Test
    public void testEmptyParentTable() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            conn.prepareStatement("DROP TABLE IF EXISTS target").execute();

            Table<?> table = ctx.meta()
                    .filterSchemas(s -> s.getName().equals("bloomdb"))
                    .filterTables(t -> !t.getName().equals("filtertype"))
                    .getTables()
                    .get(0);

            Set<Token> tokenSet = new PatternMatch(ctx, "test").tokenSet();
            BloomFilterTempTable tempTable = new BloomFilterTempTable(ctx, table, 0L, tokenSet);
            tempTable.create();
            tempTable.generateCondition();
        });
    }

    @Test
    public void testConditionGeneration() {
        fillTargetTable();
        Assertions.assertDoesNotThrow(() -> {
            Table<?> table = ctx.meta()
                    .filterSchemas(s -> s.getName().equals("bloomdb"))
                    .filterTables(t -> !t.getName().equals("filtertype"))
                    .getTables()
                    .get(0);

            Set<Token> tokenSet = new PatternMatch(ctx, "test").tokenSet();
            BloomFilterTempTable tempTable = new BloomFilterTempTable(ctx, table, 0L, tokenSet);
            tempTable.create();
            Condition condition = tempTable.generateCondition();
            String targetCondition = "(\n" +
                    "  bloommatch(\n" +
                    "    (\n" +
                    "      select \"term_0_target\".\"filter\"\n" +
                    "      from \"term_0_target\"\n" +
                    "      where (\n" +
                    "        term_id = 0\n" +
                    "        and type_id = \"bloomdb\".\"target\".\"filter_type_id\"\n" +
                    "      )\n" +
                    "    ),\n" +
                    "    \"bloomdb\".\"target\".\"filter\"\n" +
                    "  ) = true\n" +
                    "  and \"bloomdb\".\"target\".\"filter\" is not null\n" +
                    ")";
            Assertions.assertEquals(targetCondition, condition.toString());
        });
    }

    @Test
    public void testBloomTerm() {
        fillTargetTable();
        Table<?> table = ctx.meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);

        Set<Token> tokenSet = new PatternMatch(ctx, "test").tokenSet();
        BloomFilterTempTable tempTable = new BloomFilterTempTable(ctx, table, 1L, tokenSet);
        tempTable.create();
        Condition condition = tempTable.generateCondition();
        Assertions.assertTrue(condition.toString().contains("term_1_"));
    }

    void fillTargetTable() {
        Assertions.assertDoesNotThrow(() -> {
            String sql = "INSERT INTO `target` (`partition_id`, `filter_type_id`, `filter`) " +
                    "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            BloomFilter filter = BloomFilter.create(1000, 0.01);
            final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                filter.writeTo(filterBAOS);
                filterBAOS.close();
            });
            stmt.setInt(1, 1);
            stmt.setInt(2, 1);
            stmt.setBytes(3, filterBAOS.toByteArray());
            stmt.executeUpdate();
        });
    }
}