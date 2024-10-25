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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SearchTermBloomFilterTest {

    @Test
    public void testCorrectFilterSize() {
        String searchTerm = "test";
        SearchTermBloomFilter filter1 = new SearchTermBloomFilter(1000L, 0.01, new TokenizedValue(searchTerm));
        SearchTermBloomFilter filter2 = new SearchTermBloomFilter(1000L, 0.02, new TokenizedValue(searchTerm));
        SearchTermBloomFilter filter3 = new SearchTermBloomFilter(100L, 0.01, new TokenizedValue(searchTerm));
        byte[] bytes1 = Assertions.assertDoesNotThrow(filter1::bytes);
        byte[] bytes2 = Assertions.assertDoesNotThrow(filter2::bytes);
        byte[] bytes3 = Assertions.assertDoesNotThrow(filter3::bytes);
        BloomFilter resultFilter1 = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes1)));
        BloomFilter resultFilter2 = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes2)));
        BloomFilter resultFilter3 = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes3)));
        Assertions.assertEquals(BloomFilter.create(1000L, 0.01).bitSize(), resultFilter1.bitSize());
        Assertions.assertEquals(BloomFilter.create(1000L, 0.02).bitSize(), resultFilter2.bitSize());
        Assertions.assertEquals(BloomFilter.create(100L, 0.01).bitSize(), resultFilter3.bitSize());
    }

    @Test
    public void testNoRegexExtractedTokensException() {
        String searchTerm = "NoMatch";
        SearchTermBloomFilter filter = new SearchTermBloomFilter(
                1000L,
                0.01,
                new RegexExtractedValue(searchTerm, "Pattern")
        );
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, filter::bytes);
        String expectedMessage = "Trying to insert empty filter, pattern match joined table should always have tokens";
        Assertions.assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    public void testRegexExtractedTokens() {
        String searchTerm = "SearchValuePatternInThisString";
        SearchTermBloomFilter filter = new SearchTermBloomFilter(
                1000L,
                0.01,
                new RegexExtractedValue(searchTerm, "Pattern")
        );
        byte[] bytes = Assertions.assertDoesNotThrow(filter::bytes);
        BloomFilter resultFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(bytes)));
        Assertions.assertTrue(resultFilter.mightContain("Pattern"));
    }

    @Test
    public void testTokenizerTokens() {
        String searchTerm = "SearchValuePatternInThisString.Without.Delimiter";
        SearchTermBloomFilter filter = new SearchTermBloomFilter(1000L, 0.01, new TokenizedValue(searchTerm));
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
                .forClass(SearchTermBloomFilter.class)
                .withNonnullFields("expected")
                .withNonnullFields("fpp")
                .withNonnullFields("stringTokens")
                .withIgnoredFields("LOGGER")
                .verify();
    }
}
