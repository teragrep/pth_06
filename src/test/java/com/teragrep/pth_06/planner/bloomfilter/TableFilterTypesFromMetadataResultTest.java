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
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TableFilterTypesFromMetadataResultTest {

    final String userName = "sa";
    final String password = "";
    // matches IPv4
    final String ipRegex = "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    // matches IPv4 starting with 255.
    final String ipStartingWith255 = "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    final List<String> patternList = new ArrayList<>(Arrays.asList(ipRegex, ipStartingWith255));
    Connection conn;

    @BeforeEach
    public void setup() {
        final String url = "jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
        conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
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
        Assertions.assertDoesNotThrow(() -> {
            String targetTable = "CREATE TABLE `target`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(targetTable).execute();
        });
    }

    @AfterEach
    public void tearDown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testNoFilterTypes() {
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilterTypesFromMetadata result = new TableFilterTypesFromMetadata(ctx, table, 0L);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, result::toResult);
        Assertions.assertEquals("Origin table was empty", exception.getMessage());
    }

    @Test
    public void testOneFilterType() {
        insertSizedFilterIntoTargetTable(1);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilterTypesFromMetadata result = new TableFilterTypesFromMetadata(ctx, table, 0L);
        Result<Record> records = result.toResult();
        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals(ULong.valueOf(1000), records.get(0).get(1));
        Assertions.assertEquals(0.01, records.get(0).get(2));
    }

    @Test
    public void testMultipleFilterTypes() {
        insertSizedFilterIntoTargetTable(1);
        insertSizedFilterIntoTargetTable(2);
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilterTypesFromMetadata result = new TableFilterTypesFromMetadata(ctx, table, 0L);
        Result<Record> records = result.toResult();
        Assertions.assertEquals(2, records.size());
        Record first = records.get(0);
        Record second = records.get(1);
        Assertions.assertEquals(first.get(1), ULong.valueOf("1000"));
        Assertions.assertEquals(second.get(1), ULong.valueOf("2000"));
    }

    @Test
    public void testNullTableException() {
        DSLContext ctx = DSL.using(conn);
        Assertions.assertThrows(NullPointerException.class, () -> new TableFilterTypesFromMetadata(ctx, null, 0L));
    }

    @Test
    public void testEquality() {
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilterTypesFromMetadata result1 = new TableFilterTypesFromMetadata(ctx, table, 0L);
        TableFilterTypesFromMetadata result2 = new TableFilterTypesFromMetadata(ctx, table, 0L);
        Assertions.assertEquals(result1, result2);
    }

    @Test
    public void testNotEquals() {
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilterTypesFromMetadata result1 = new TableFilterTypesFromMetadata(ctx, table, 0L);
        TableFilterTypesFromMetadata result2 = new TableFilterTypesFromMetadata(ctx, table, 1L);
        Assertions.assertNotEquals(result1, result2);
    }

    @Test
    public void testHashCode() {
        DSLContext ctx = DSL.using(conn);
        Table<?> table = ctx
                .meta()
                .filterSchemas(s -> s.getName().equals("bloomdb"))
                .filterTables(t -> !t.getName().equals("filtertype"))
                .getTables()
                .get(0);
        TableFilterTypesFromMetadata result1 = new TableFilterTypesFromMetadata(ctx, table, 0L);
        TableFilterTypesFromMetadata result2 = new TableFilterTypesFromMetadata(ctx, table, 0L);
        TableFilterTypesFromMetadata notEq = new TableFilterTypesFromMetadata(ctx, table, 1L);
        Assertions.assertEquals(result1.hashCode(), result2.hashCode());
        Assertions.assertNotEquals(result1.hashCode(), notEq.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(TableFilterTypesFromMetadata.class)
                .withNonnullFields("ctx", "table", "expectedField", "fppField", "patternField", "filterTypeIdField")
                .verify();
    }

    private void insertSizedFilterIntoTargetTable(int filterTypeId) {
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
            stmt.setInt(1, filterTypeId);
            stmt.setInt(2, filterTypeId);
            stmt.setBytes(3, filterBAOS.toByteArray());
            int success = stmt.executeUpdate();
            Assertions.assertEquals(1, success);
            stmt.close();
        });
    }
}
