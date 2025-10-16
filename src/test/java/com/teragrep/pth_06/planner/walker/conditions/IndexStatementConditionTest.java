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
package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 * 
 * @see org.jooq.QueryPart
 */
public class IndexStatementConditionTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    // matches IPv4
    final String ipRegex = "(\\b25[0-5]|\\b2[0-4][0-9]|\\b[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    // matches IPv4 starting with 255.
    final String ipStartingWith255 = "(\\b25[0-5]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";
    final List<String> patternList = new ArrayList<>(Arrays.asList(ipRegex, ipStartingWith255));
    final ConditionConfig mockConfig = new ConditionConfig(
            DSL.using(new MockConnection(ctx -> new MockResult[0])),
            false,
            true
    );
    Connection conn;

    @BeforeEach
    public void setup() {
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
            String ip = "CREATE TABLE `pattern_test_ip`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            String ip255 = "CREATE TABLE `pattern_test_ip255`("
                    + "    `id`             bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                    + "    `partition_id`   bigint(20) unsigned NOT NULL UNIQUE,"
                    + "    `filter_type_id` bigint(20) unsigned NOT NULL,"
                    + "    `filter`         longblob            NOT NULL)";
            conn.prepareStatement(filtertype).execute();
            conn.prepareStatement(ip).execute();
            conn.prepareStatement(ip255).execute();
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
            writeFilter("pattern_test_ip", 1);
            writeFilter("pattern_test_ip255", 2);
        });
    }

    @AfterEach
    public void tearDown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testConnectionException() {
        DSLContext ctx = DSL.using(new MockConnection(c -> new MockResult[0]));
        ConditionConfig config = new ConditionConfig(ctx, false, true);
        ConditionConfig noBloomConfig = new ConditionConfig(ctx, false);
        IndexStatementCondition cond1 = new IndexStatementCondition("test", config, DSL.trueCondition());
        IndexStatementCondition cond2 = new IndexStatementCondition("test", noBloomConfig, DSL.trueCondition());
        Assertions.assertThrows(DataAccessException.class, cond1::condition);
        Assertions.assertDoesNotThrow(cond2::condition);
    }

    @Test
    public void noMatchesTest() {
        DSLContext ctx = DSL.using(conn);
        Condition e1 = DSL.falseCondition();
        Condition e2 = DSL.trueCondition();
        ConditionConfig config = new ConditionConfig(ctx, false, true);
        ConditionConfig withoutFiltersConfig = new ConditionConfig(ctx, false, true, false, "", 0L);
        IndexStatementCondition cond1 = new IndexStatementCondition("test", config, e1);
        IndexStatementCondition cond2 = new IndexStatementCondition("test", withoutFiltersConfig, e2);
        Assertions.assertEquals(e1, cond1.condition());
        Assertions.assertEquals(e2, cond2.condition());
        Assertions.assertTrue(cond1.requiredTables().isEmpty());
        Assertions.assertTrue(cond2.requiredTables().isEmpty());
    }

    @Test
    public void oneMatchingTableTest() {
        DSLContext ctx = DSL.using(conn);
        ConditionConfig config = new ConditionConfig(ctx, false, true);
        IndexStatementCondition cond = new IndexStatementCondition("192.168.1.1", config);
        String e = "(\n" + "  (\n" + "    bloommatch(\n" + "      (\n"
                + "        select \"term_0_pattern_test_ip\".\"filter\"\n" + "        from \"term_0_pattern_test_ip\"\n"
                + "        where (\n" + "          term_id = 0\n"
                + "          and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "        )\n"
                + "      ),\n" + "      \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "    ) = true\n"
                + "    and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "  )\n"
                + "  or \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n" + ")";
        Assertions.assertEquals(e, cond.condition().toString());
        Assertions.assertEquals(1, cond.requiredTables().size());
    }

    @Test
    public void twoMatchingTableTest() {
        DSLContext ctx = DSL.using(conn);
        ConditionConfig config = new ConditionConfig(ctx, false, true);
        IndexStatementCondition cond = new IndexStatementCondition("255.255.255.255", config);
        String e = "(\n" + "  (\n" + "    bloommatch(\n" + "      (\n"
                + "        select \"term_0_pattern_test_ip\".\"filter\"\n" + "        from \"term_0_pattern_test_ip\"\n"
                + "        where (\n" + "          term_id = 0\n"
                + "          and type_id = \"bloomdb\".\"pattern_test_ip\".\"filter_type_id\"\n" + "        )\n"
                + "      ),\n" + "      \"bloomdb\".\"pattern_test_ip\".\"filter\"\n" + "    ) = true\n"
                + "    and \"bloomdb\".\"pattern_test_ip\".\"filter\" is not null\n" + "  )\n" + "  or (\n"
                + "    bloommatch(\n" + "      (\n" + "        select \"term_0_pattern_test_ip255\".\"filter\"\n"
                + "        from \"term_0_pattern_test_ip255\"\n" + "        where (\n" + "          term_id = 0\n"
                + "          and type_id = \"bloomdb\".\"pattern_test_ip255\".\"filter_type_id\"\n" + "        )\n"
                + "      ),\n" + "      \"bloomdb\".\"pattern_test_ip255\".\"filter\"\n" + "    ) = true\n"
                + "    and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is not null\n" + "  )\n" + "  or (\n"
                + "    \"bloomdb\".\"pattern_test_ip\".\"filter\" is null\n"
                + "    and \"bloomdb\".\"pattern_test_ip255\".\"filter\" is null\n" + "  )\n" + ")";
        Assertions.assertEquals(e, cond.condition().toString());
        Assertions.assertEquals(2, cond.requiredTables().size());
    }

    @Test
    public void equalsTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", mockConfig);
        IndexStatementCondition eq2 = new IndexStatementCondition("946677600", mockConfig);
        Assertions.assertEquals(eq1, eq2);
    }

    @Test
    public void notEqualsTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", mockConfig);
        IndexStatementCondition notEq = new IndexStatementCondition("1000", mockConfig);
        Assertions.assertNotEquals(eq1, notEq);
    }

    @Test
    public void hashCodeTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", mockConfig);
        IndexStatementCondition eq2 = new IndexStatementCondition("946677600", mockConfig);
        IndexStatementCondition notEq = new IndexStatementCondition("1000", mockConfig);
        Assertions.assertEquals(eq1.hashCode(), eq2.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), notEq.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(IndexStatementCondition.class)
                .withNonnullFields("value")
                .withNonnullFields("config")
                .withNonnullFields("condition")
                .withNonnullFields("tableSet")
                .withIgnoredFields("LOGGER")
                .verify();
    }

    private void writeFilter(String tableName, int filterId) {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS BLOOMDB").execute();
            conn.prepareStatement("USE BLOOMDB").execute();
            String sql = "INSERT INTO `" + tableName + "` (`partition_id`, `filter_type_id`, `filter`) "
                    + "VALUES (?, (SELECT `id` FROM `filtertype` WHERE id=?), ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            BloomFilter filter = BloomFilter.create(1000, 0.01);
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
        });
    }
}
