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
package com.teragrep.pth_06.planner.factory;

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.ArchiveQuery;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public final class ArchiveQueryFactoryTest {

    private final Map<String, String> opts = new HashMap<>();

    @BeforeEach
    public void setup() {
        final String query = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><AND><earliest operation=\"EQUALS\" value=\"1262296800\"/><latest operation=\"EQUALS\" value=\"1263679201\"/></AND></AND>";
        opts.put("archive.enabled", "true");
        opts.put("queryXML", query);
        opts.put("S3endPoint", "http://127.0.0.1:48080");
        opts.put("S3identity", "s3identity");
        opts.put("S3credential", "s3credential");
        opts.put("DBusername", "sa");
        opts.put("DBpassword", "");
        opts.put("DBurl", "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE");
        opts.put("kafka.enabled", "false");
        opts
                .put(
                        "kafka.bootstrap.servers",
                        "kafkadev01.example.com:9092,kafkadev02.example.com:9092,kafkadev03.example.com:9092"
                );
        opts.put("kafka.sasl.mechanism", "PLAIN");
        opts.put("kafka.security.protocol", "SASL_PLAINTEXT");
        opts
                .put(
                        "kafka.sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"superuser\" password=\"SuperSecretSuperuserPassword\";"
                );
    }

    @Test
    public void testArchiveEnabled() {
        Factory<ArchiveQuery> archiveQueryFactory = new ArchiveQueryFactory(new Config(opts));
        // tests that ArchiveQuery tries to initialize StreamDBClient class resulting in sql DataAccessException
        DataAccessException dataAccessException = Assertions
                .assertThrows(DataAccessException.class, archiveQueryFactory::object);
        String expectedMessage = "SQL [drop temporary table if exists `getArchivedObjects_filter_table`]; Syntax error in SQL statement \"drop [*]temporary table if exists `getArchivedObjects_filter_table`\"; expected \"TABLE, INDEX, USER, SEQUENCE, CONSTANT, TRIGGER, MATERIALIZED, VIEW, ROLE, ALIAS, SCHEMA, ALL OBJECTS, DOMAIN, TYPE, AGGREGATE, SYNONYM\"; SQL statement:\n"
                + "drop temporary table if exists `getArchivedObjects_filter_table` [42001-224]";
        Assertions.assertEquals(expectedMessage, dataAccessException.getMessage());
    }

    @Test
    public void testArchiveDisabled() {
        Map<String, String> archiveDisabledOpts = new HashMap<>(opts);
        archiveDisabledOpts.put("archive.enabled", "false");
        archiveDisabledOpts.put("kafka.enabled", "true"); // required to have some datasource enabled
        Factory<ArchiveQuery> archiveQueryFactory = new ArchiveQueryFactory(new Config(archiveDisabledOpts));
        ArchiveQuery archiveQuery = archiveQueryFactory.object();
        Assertions.assertTrue(archiveQuery.isStub());
    }

    @Test
    public void testArchiveDisabledWhenHbaseIsEnabled() {
        Map<String, String> hbaseEnabledOpts = new HashMap<>(opts);
        hbaseEnabledOpts.put("hbase.enabled", "true");
        hbaseEnabledOpts.put("archive.enabled", "true");
        Factory<ArchiveQuery> archiveQueryFactory = new ArchiveQueryFactory(new Config(hbaseEnabledOpts));
        ArchiveQuery archiveQuery = archiveQueryFactory.object();
        Assertions.assertTrue(archiveQuery.isStub());
    }

}
