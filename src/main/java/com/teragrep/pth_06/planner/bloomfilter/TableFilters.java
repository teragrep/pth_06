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

import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static org.jooq.impl.SQLDataType.BIGINTUNSIGNED;

/**
 * Filter types of a table that can be inserted into the category table
 */
public final class TableFilters {

    private final TableRecords recordsInMetadata;
    private final DSLContext ctx;
    private final Table<?> table;
    private final Table<?> thisTable;
    private final String searchTerm;
    private final long bloomTermId;

    public TableFilters(DSLContext ctx, Table<?> table, long bloomTermId, String searchTerm) {
        this(
                new TableFilterTypesFromMetadata(ctx, table, bloomTermId),
                ctx,
                table,
                DSL.table(DSL.name(("term_" + bloomTermId + "_" + table.getName()))),
                bloomTermId,
                searchTerm
        );
    }

    public TableFilters(
            TableFilterTypesFromMetadata recordsInMetadata,
            DSLContext ctx,
            Table<?> table,
            Table<?> thisTable,
            long bloomTermId,
            String searchTerm
    ) {
        this.recordsInMetadata = recordsInMetadata;
        this.ctx = ctx;
        this.table = table;
        this.thisTable = thisTable;
        this.bloomTermId = bloomTermId;
        this.searchTerm = searchTerm;
    }

    public SafeBatch asBatch() {
        if (table == null) {
            throw new IllegalStateException("Origin table was null");
        }
        final List<InsertValuesStepN<?>> insertValuesStepNList = new ArrayList<>();
        final Result<Record> result = recordsInMetadata.toResult();
        for (final Record record : result) {
            final Field<?>[] insertFields = {
                    DSL.field(DSL.name(thisTable.getName(), "term_id"), BIGINTUNSIGNED.nullable(false)),
                    DSL.field(DSL.name(thisTable.getName(), "type_id"), BIGINTUNSIGNED.nullable(false)),
                    DSL.field(DSL.name(thisTable.getName(), "filter"), byte[].class)
            };
            final ULong expectedField = record
                    .getValue(DSL.field(DSL.name(table.getName(), "expectedElements"), ULong.class));
            final Double fpp = record.getValue(DSL.field(DSL.name(table.getName(), "targetFpp"), Double.class));
            final String pattern = record.getValue(BLOOMDB.FILTERTYPE.PATTERN, String.class);
            final SearchTermBloomFilter filter;
            if (pattern == null) {
                filter = new SearchTermBloomFilter(expectedField.longValue(), fpp, new TokenizedValue(searchTerm));
            }
            else {
                filter = new SearchTermBloomFilter(
                        expectedField.longValue(),
                        fpp,
                        new RegexExtractedValue(searchTerm, pattern)
                );
            }
            final Field<?>[] valueFields = {
                    DSL.val(bloomTermId, ULong.class),
                    DSL.val(record.getValue(BLOOMDB.FILTERTYPE.ID), ULong.class),
                    DSL.val(filter.bytes(), byte[].class)
            };
            final InsertValuesStepN<?> insertStep = ctx.insertInto(thisTable).columns(insertFields).values(valueFields);
            insertValuesStepNList.add(insertStep);
        }
        final Batch batch = ctx.batch(insertValuesStepNList);
        return new SafeBatch(batch);
    }

    /**
     * Equal if the compared object is the same instance or if the compared object is of the same class, object fields
     * are equal, and DSLContext is the same instance
     *
     * @param object object compared against
     * @return true if equal
     */
    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || getClass() != object.getClass())
            return false;
        final TableFilters cast = (TableFilters) object;
        return bloomTermId == cast.bloomTermId && recordsInMetadata
                .equals(cast.recordsInMetadata) && ctx == cast.ctx && table.equals(cast.table)
                && thisTable.equals(cast.thisTable) && searchTerm.equals(cast.searchTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsInMetadata, ctx, table, thisTable, searchTerm, bloomTermId);
    }
}
