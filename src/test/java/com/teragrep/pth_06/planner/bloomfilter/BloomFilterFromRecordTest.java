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
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BloomFilterFromRecordTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));

    @Test
    public void testInstantation() {
        Record dynamicRecord = generateRecord(true);
        Table<?> target = DSL.table(DSL.name("target"));
        String searchTerm = "Pattern";
        BloomFilterFromRecord filter = new BloomFilterFromRecord(dynamicRecord, target, searchTerm);
        Assertions.assertDoesNotThrow(filter::bytes);
    }

    @Test
    public void testCorrectFilterSize() {
        Record dynamicRecord = generateRecord(true);
        Table<?> target = DSL.table(DSL.name("target"));
        String searchTerm = "SearchValuePatternInThisString";
        BloomFilterFromRecord filter = new BloomFilterFromRecord(dynamicRecord, target, searchTerm);
        byte[] bytes = Assertions.assertDoesNotThrow(filter::bytes);
        BloomFilter resultFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes)));
        BloomFilter expectedSize = BloomFilter.create(100, 0.01);
        Assertions.assertEquals(expectedSize.bitSize(), resultFilter.bitSize());
    }

    @Test
    public void testNoRegexExtractedTokensException() {
        Record dynamicRecord = generateRecord(true);
        Table<?> target = DSL.table(DSL.name("target"));
        String searchTerm = "NoMatch";
        BloomFilterFromRecord filter = new BloomFilterFromRecord(dynamicRecord, target, searchTerm);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, filter::bytes);
        String expectedMessage = "Trying to insert empty filter, pattern match joined table should always have tokens";
        Assertions.assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    public void testRegexExtractedTokens() {
        Record dynamicRecord = generateRecord(true);
        Table<?> target = DSL.table(DSL.name("target"));
        String searchTerm = "SearchValuePatternInThisString";
        BloomFilterFromRecord filter = new BloomFilterFromRecord(dynamicRecord, target, searchTerm);
        byte[] bytes = Assertions.assertDoesNotThrow(filter::bytes);
        BloomFilter resultFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes)));
        Assertions.assertTrue(resultFilter.mightContain("Pattern"));
    }

    @Test
    public void testTokenizerTokens() {
        Record dynamicRecord = generateRecord(false);
        Table<?> target = DSL.table(DSL.name("target"));
        String searchTerm = "SearchValuePatternInThisString.Without.Delimiter";
        BloomFilterFromRecord filter = new BloomFilterFromRecord(dynamicRecord, target, searchTerm);
        byte[] bytes = Assertions.assertDoesNotThrow(filter::bytes);
        BloomFilter resultFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes)));
        Assertions.assertFalse(resultFilter.mightContain("Pattern"));
        Assertions.assertTrue(resultFilter.mightContain("Without"));
        Assertions.assertTrue(resultFilter.mightContain("SearchValuePatternInThisString"));
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(BloomFilterFromRecord.class)
                .withNonnullFields("expected")
                .withNonnullFields("fpp")
                .withNonnullFields("searchTerm")
                .withIgnoredFields("LOGGER")
                .verify();
    }

    private Record generateRecord(final boolean withPattern) {
        DSLContext ctx = DSL.using(conn);
        Field<ULong> idField = DSL.field(DSL.name("id"), ULong.class);
        Field<ULong> expectedField = DSL.field(DSL.name("expectedElements"), ULong.class);
        Field<Double> fppField = DSL.field(DSL.name("targetFpp"), Double.class);
        Field<String> patternField = DSL.field(DSL.name("pattern"), String.class);

        Record dynamicRecord = ctx.newRecord(idField, expectedField, fppField, patternField);
        if (withPattern) {
            dynamicRecord.set(patternField, "Pattern");
        }
        else {
            // case is joined filtertype table has no pattern
            dynamicRecord.set(patternField, null);
        }
        dynamicRecord.set(idField, ULong.valueOf(1));
        dynamicRecord.set(expectedField, ULong.valueOf(100));
        dynamicRecord.set(fppField, 0.01);
        return dynamicRecord;
    }
}
