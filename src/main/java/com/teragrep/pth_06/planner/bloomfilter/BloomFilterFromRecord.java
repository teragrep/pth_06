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

import com.teragrep.pth_06.planner.RegexExtractedValue;
import com.teragrep.pth_06.planner.TokenizedValue;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

/**
 * Extracts filter type from record, creates a bloom filter and returns the filters byte array
 */
public final class BloomFilterFromRecord {

    private final Logger LOGGER = LoggerFactory.getLogger(BloomFilterFromRecord.class);
    private final Long expected;
    private final Double fpp;
    private final String pattern;
    private final String searchTerm;

    private BloomFilter create() {
        if (expected == null || fpp == null) {
            LOGGER
                    .error(
                            "Null field while creating bloom filter expected <{}>, fpp <{}>, pattern <{}>, search term <{}>",
                            expected, fpp, pattern, searchTerm
                    );
            throw new RuntimeException("Object field was null");
        }
        final BloomFilter filter = BloomFilter.create(expected, fpp);
        // if no pattern use to tokenized value (currently BLOOMDB.FILTERTYPE.PATTERN is NOT NULL)
        if (pattern == null) {
            LOGGER.info("Table pattern was null using tokenizer to generate tokens");
            new TokenizedValue(searchTerm).stringTokens().forEach(filter::put);
        }
        else { // get tokens using regex
            final Set<String> tokens = new RegexExtractedValue(searchTerm, pattern).tokens();
            LOGGER.info("Insert pattern <{}> tokens to temp table filter <{}>", pattern, tokens);
            if (tokens.isEmpty()) {
                throw new IllegalStateException(
                        "Trying to insert empty filter, pattern match joined table should always have tokens"
                );
            }
            tokens.forEach(filter::put);
        }
        return filter;
    }

    public BloomFilterFromRecord(Record record, Table<?> table, String searchTerm) {
        this(
                record.getValue(DSL.field(DSL.name(table.getName(), "expectedElements"), ULong.class)).longValue(),
                record.getValue(DSL.field(DSL.name(table.getName(), "targetFpp"), Double.class)),
                record.getValue(BLOOMDB.FILTERTYPE.PATTERN, String.class),
                searchTerm
        );
    }

    public BloomFilterFromRecord(Long expected, Double fpp, String pattern, String searchTerm) {
        this.expected = expected;
        this.fpp = fpp;
        this.pattern = pattern;
        this.searchTerm = searchTerm;
    }

    public byte[] bytes() {
        final BloomFilter filter = create();
        final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
        try {
            filter.writeTo(filterBAOS);
            filterBAOS.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(new IOException("Error writing filter bytes: " + e.getMessage()));
        }
        return filterBAOS.toByteArray();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || getClass() != object.getClass())
            return false;
        final BloomFilterFromRecord cast = (BloomFilterFromRecord) object;
        return expected.equals(cast.expected) && fpp.equals(cast.fpp) && Objects.equals(pattern, cast.pattern)
                && searchTerm.equals(cast.searchTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expected, fpp, pattern, searchTerm);
    }
}
