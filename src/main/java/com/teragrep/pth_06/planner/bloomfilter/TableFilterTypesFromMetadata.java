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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

/**
 * Filter types of a table from metadata
 */
public final class TableFilterTypesFromMetadata implements TableRecords {

    private final DSLContext ctx;
    private final Table<?> table;
    private final Field<ULong> expectedField;
    private final Field<Double> fppField;
    private final Field<String> patternField;
    private final Field<ULong> filterTypeIdField;

    public TableFilterTypesFromMetadata(DSLContext ctx, Table<?> table, long bloomTermId) {
        this(
                ctx,
                table.join(BLOOMDB.FILTERTYPE).on(BLOOMDB.FILTERTYPE.ID.eq(table.field("filter_type_id").cast(ULong.class))), DSL.table(DSL.name(("term_" + bloomTermId + "_" + table.getName()))).getName()
        );
    }

    public TableFilterTypesFromMetadata(DSLContext ctx, Table<?> table, String name) {
        this(
                ctx,
                table,
                DSL.field(DSL.name(name, "expectedElements"), ULong.class),
                DSL.field(DSL.name(name, "targetFpp"), Double.class),
                DSL.field(DSL.name(name, "pattern"), String.class)
        );

    }

    public TableFilterTypesFromMetadata(
            DSLContext ctx,
            Table<?> table,
            Field<ULong> expectedField,
            Field<Double> fppField,
            Field<String> patternField
    ) {
        this.ctx = ctx;
        this.table = table;
        this.expectedField = expectedField;
        this.fppField = fppField;
        this.patternField = patternField;
        this.filterTypeIdField = table.field("filter_type_id").cast(ULong.class);
    }

    public Result<Record> toResult() {
        List<Field<?>> selectFieldsList = Arrays
                .asList(BLOOMDB.FILTERTYPE.ID, table.field("expectedElements"), table.field("targetFpp"), table.field("pattern"));
        // Fetch filtertype values from metadata
        final Result<Record> records = ctx.select(selectFieldsList).from(table).groupBy(filterTypeIdField).fetch();
        if (records.isEmpty()) {
            throw new RuntimeException("Origin table was empty");
        }
        return records;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || getClass() != object.getClass())
            return false;
        final TableFilterTypesFromMetadata cast = (TableFilterTypesFromMetadata) object;
        return ctx == cast.ctx && Objects.equals(table, cast.table) && Objects
                .equals(expectedField, cast.expectedField) && Objects.equals(fppField, cast.fppField) && Objects
                        .equals(patternField, cast.patternField)
                && Objects.equals(filterTypeIdField, cast.filterTypeIdField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, table, expectedField, fppField, patternField, filterTypeIdField);
    }
}
