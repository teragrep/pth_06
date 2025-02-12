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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TableFiltersTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    // matches IPv4
    final String ipRegex = "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    // matches with values surrounded by parentheses
    final String parenthesesPattern = "\\((.*?)\\)";
    final List<String> patternList = new ArrayList<>(Arrays.asList(ipRegex, parenthesesPattern));
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS filtertype").execute();
            String filtertype = "CREATE TABLE`filtertype`" + "("
                    + "    `id`               bigint(20) unsigned   NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `expectedElements` bigint(20) unsigned NOT NULL,"
                    + "    `targetFpp`        DOUBLE(2) unsigned NOT NULL,"
                    + "    `pattern`          VARCHAR(2048) NOT NULL,"
                    + "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`)" + ")";
            conn.prepareStatement(filtertype).execute();
            String typeSQL = "INSERT INTO `filtertype` (`id`,`expectedElements`, `targetFpp`, `pattern`) VALUES (?,?,?,?)";
            int id = 1;
            for (String pattern : patternList) {
                PreparedStatement filterType = conn.prepareStatement(typeSQL);
                filterType.setInt(1, id);
                filterType.setInt(2, id * 1000);
                filterType.setDouble(3, 0.01);
                filterType.setString(4, pattern);
                filterType.executeUpdate();
                id++;
            }
        });
    }

    @BeforeEach
    public void createTargetTable() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS target").execute();
            // drop temp tables created by tests
            conn.prepareStatement("DROP TABLE IF EXISTS term_0_target").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS term_1_target").execute();
            String targetTable = "CREATE TABLE `target`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(targetTable).execute();
        });
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testCreation() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        Assertions.assertDoesNotThrow(() -> new TableFilters(ctx, table, 0L, "test"));
    }

    @Test
    public void testFilterInsertion() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        CategoryTable tableImpl = new CategoryTableImpl(ctx, table, 0L, "192.168.1.1");
        Assertions.assertDoesNotThrow(tableImpl::create);
        Assertions.assertDoesNotThrow(() -> new TableFilters(ctx, table, 0L, "192.168.1.1").asBatch().execute());
        ResultSet result = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM `term_0_target`").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int loops = 0;
            while (result.next()) {
                long termId = result.getLong("term_id");
                long typeId = result.getLong("type_id");
                byte[] filterBytes = result.getBytes("filter");
                Assertions.assertEquals(0, termId);
                Assertions.assertEquals(1, typeId);
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(filterBytes));
                Assertions.assertTrue(filter.mightContain("192.168.1.1"));
                Assertions.assertFalse(filter.mightContain("192"));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result.close();
        });
    }

    @Test
    public void testFilterInsertionWithRegexExtractedValue() {
        fillTargetTable(2);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        String value = "biz baz boz data has no content today (very important though) but it would still have if one had a means to extract it from (here is something else important as well) the strange patterns called parentheses that it seems to have been put in.";
        CategoryTable tableImpl = new CategoryTableImpl(ctx, table, 1L, value);
        Assertions.assertDoesNotThrow(tableImpl::create);
        Assertions.assertDoesNotThrow(() -> new TableFilters(ctx, table, 1L, value).asBatch().execute());
        ResultSet result = Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM `term_1_target`").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int loops = 0;
            while (result.next()) {
                long termId = result.getLong("term_id");
                long typeId = result.getLong("type_id");
                byte[] filterBytes = result.getBytes("filter");
                Assertions.assertEquals(1, termId);
                Assertions.assertEquals(2, typeId);
                BloomFilter filter = BloomFilter.readFrom(new ByteArrayInputStream(filterBytes));
                Assertions.assertTrue(filter.mightContain("(here is something else important as well)"));
                Assertions.assertTrue(filter.mightContain("(very important though)"));
                Assertions.assertFalse(filter.mightContain("content"));
                Assertions.assertFalse(filter.mightContain("(very"));
                Assertions.assertFalse(filter.mightContain("though)"));
                loops++;
            }
            Assertions.assertEquals(1, loops);
            result.close();
        });
    }

    @Test
    public void testMissingTempTableDataAccessException() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        DataAccessException ex = Assertions
                .assertThrows(DataAccessException.class, () -> new TableFilters(ctx, table, 0L, "192.168.1.1").asBatch().execute());
        Assertions.assertTrue(ex.getMessage().contains("Table \"term_0_target\" not found"));
    }

    @Test
    public void testCreateFilterWithoutItemsException() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        IllegalStateException exception = Assertions
                .assertThrows(IllegalStateException.class, () -> new TableFilters(ctx, table, 0L, "nomatch").asBatch().execute());
        String expected = "Tried to create a filter without any items";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    public void testEquals() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);

        TableFilters filter1 = new TableFilters(ctx, table, 0L, "test");
        TableFilters filter2 = new TableFilters(ctx, table, 0L, "test");
        Assertions.assertEquals(filter1, filter2);
    }

    @Test
    public void testNotEquals() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);

        TableFilters filter1 = new TableFilters(ctx, table, 0L, "test");
        TableFilters filter2 = new TableFilters(ctx, table, 1L, "test");
        TableFilters filter3 = new TableFilters(ctx, table, 0L, "mest");
        Assertions.assertNotEquals(filter1, filter2);
        Assertions.assertNotEquals(filter1, filter3);
    }

    @Test
    public void testHashCode() {
        fillTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilters filter1 = new TableFilters(ctx, table, 0L, "test");
        TableFilters filter2 = new TableFilters(ctx, table, 0L, "test");
        TableFilters notEq1 = new TableFilters(ctx, table, 0L, "notTest");
        TableFilters notEq2 = new TableFilters(ctx, table, 1L, "test");
        Assertions.assertEquals(filter1.hashCode(), filter2.hashCode());
        Assertions.assertNotEquals(filter1.hashCode(), notEq1.hashCode());
        Assertions.assertNotEquals(filter1.hashCode(), notEq2.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(TableFilters.class)
                .withNonnullFields("recordsInMetadata")
                .withNonnullFields("ctx")
                .withNonnullFields("table")
                .withNonnullFields("searchTerm")
                .withNonnullFields("bloomTermId")
                .withNonnullFields("thisTable")
                .verify();
    }

    void fillTargetTable(int id) {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            String sql = "INSERT INTO `target` (`partition_id`, `filter_type_id`, `filter`) "
                    + "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            BloomFilter filter = BloomFilter.create(1000, 0.01);
            final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                filter.writeTo(filterBAOS);
                filterBAOS.close();
            });
            stmt.setInt(1, 1);
            stmt.setInt(2, id); // filter type id
            stmt.setBytes(3, filterBAOS.toByteArray());
            stmt.executeUpdate();
            stmt.close();
        });
    }
}
