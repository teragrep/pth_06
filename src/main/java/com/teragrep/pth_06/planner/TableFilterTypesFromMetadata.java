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

import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

/**
 * Filter types of a table from metadata
 */
public final class TableFilterTypesFromMetadata implements TableRecords {

    private final DSLContext ctx;
    private final Table<?> table;
    private final long bloomTermId;

    public TableFilterTypesFromMetadata(DSLContext ctx, Table<?> table, long bloomTermId) {
        this.ctx = ctx;
        this.table = table;
        this.bloomTermId = bloomTermId;
    }

    public Result<Record> toResult() {
        if (table == null) {
            throw new IllegalStateException("Origin table was null");
        }
        final Table<?> joined = table
                .join(BLOOMDB.FILTERTYPE)
                .on(BLOOMDB.FILTERTYPE.ID.eq((Field<ULong>) table.field("filter_type_id")));
        final Table<Record> namedTable = DSL.table(DSL.name(("term_" + bloomTermId + "_" + table.getName())));
        final Field<ULong> expectedField = DSL.field(DSL.name(namedTable.getName(), "expectedElements"), ULong.class);
        final Field<Double> fppField = DSL.field(DSL.name(namedTable.getName(), "targetFpp"), Double.class);
        final SelectField<?>[] resultFields = {
                BLOOMDB.FILTERTYPE.ID,
                joined.field("expectedElements").as(expectedField),
                joined.field("targetFpp").as(fppField),
                joined.field("pattern")
        };
        // Fetch filtertype values from metadata
        final Result<Record> records = ctx
                .select(resultFields)
                .from(joined)
                .groupBy(joined.field("filter_type_id"))
                .fetch();
        if (records.isEmpty()) {
            throw new RuntimeException("Origin table was empty");
        }
        return records;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || object.getClass() != this.getClass())
            return false;
        final TableFilterTypesFromMetadata cast = (TableFilterTypesFromMetadata) object;
        return bloomTermId == cast.bloomTermId && table.equals(cast.table) && ctx == cast.ctx;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, table, bloomTermId);
    }
}
