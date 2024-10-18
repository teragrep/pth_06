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
package com.teragrep.pth_06.planner;

import com.teragrep.blf_01.Token;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static org.jooq.impl.SQLDataType.BIGINTUNSIGNED;

/**
 * Filter types of a table that can be inserted into the tables category table
 */
public final class TableFilters {

    private final DSLContext ctx;
    private final Table<?> table;
    private final long bloomTermId;
    private final TokenizedValue value;
    private final TableRecords recordsInMetadata;

    public TableFilters(DSLContext ctx, Table<?> table, long bloomTermId, String input) {
        this(
                ctx,
                table,
                bloomTermId,
                new TokenizedValue(input),
                new TableFilterTypesFromMetadata(ctx, table, bloomTermId)
        );
    }

    public TableFilters(
            DSLContext ctx,
            Table<?> table,
            long bloomTermId,
            TokenizedValue value,
            TableFilterTypesFromMetadata recordsInMetadata
    ) {
        this.ctx = ctx;
        this.table = table;
        this.bloomTermId = bloomTermId;
        this.value = value;
        this.recordsInMetadata = recordsInMetadata;
    }

    /**
     * Extracts filter type from record, creates a bloom filter and returns the filters byte array
     * 
     * @param record record with filter info
     * @return byte[] of the created filter
     */
    private byte[] filterBytesFromRecord(final Record record) {
        final ULong expected = record.getValue(DSL.field(DSL.name(table.getName(), "expectedElements"), ULong.class));
        final Double fpp = record.getValue(DSL.field(DSL.name(table.getName(), "targetFpp"), Double.class));
        final String pattern = record.getValue(BLOOMDB.FILTERTYPE.PATTERN, String.class);
        final BloomFilter filter = BloomFilter.create(expected.longValue(), fpp);
        final Pattern compiled = Pattern.compile(pattern);
        boolean isEmpty = true;
        for (final Token token : value.tokens()) {
            final String tokenString = token.toString();
            if (compiled.matcher(tokenString).matches()) {
                isEmpty = false;
                filter.put(tokenString);
            }
        }
        if (isEmpty) {
            throw new IllegalStateException("Trying to insert empty filter");
        }
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

    private void insertFilterRecordToCategoryTable(final Record record) {
        final Table<Record> categoryTable = DSL.table(DSL.name(("term_" + bloomTermId + "_" + this.table.getName())));
        final Field<?>[] insertFields = {
                DSL.field("term_id", BIGINTUNSIGNED.nullable(false)),
                DSL.field("type_id", BIGINTUNSIGNED.nullable(false)),
                DSL.field(DSL.name(categoryTable.getName(), "filter"), byte[].class)
        };
        final Field<?>[] valueFields = {
                DSL.val(bloomTermId, ULong.class),
                DSL.val(record.getValue(BLOOMDB.FILTERTYPE.ID), ULong.class),
                DSL.val(filterBytesFromRecord(record), byte[].class)
        };
        ctx.insertInto(categoryTable).columns(insertFields).values(valueFields).execute();
    }

    public void insertFiltersIntoCategoryTable() {
        recordsInMetadata.toResult().forEach(this::insertFilterRecordToCategoryTable);
    }

    /**
     * Expects DSLContext values to be the same instance
     * 
     * @param object object compared
     * @returs true if object is equal
     */
    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || object.getClass() != this.getClass())
            return false;
        final TableFilters cast = (TableFilters) object;
        return ctx == cast.ctx && value.equals(cast.value) && table.equals(cast.table)
                && recordsInMetadata.equals(cast.recordsInMetadata) && bloomTermId == cast.bloomTermId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, table, bloomTermId, value, recordsInMetadata);
    }
}
