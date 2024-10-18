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

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import java.util.Objects;
import java.util.function.Consumer;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static org.jooq.impl.SQLDataType.BIGINTUNSIGNED;

public final class FilterFromRecordToCategoryTableConsumer implements Consumer<Record> {

    private final DSLContext ctx;
    private final Table<?> table;
    private final long bloomTermId;
    private final String searchTerm;

    public FilterFromRecordToCategoryTableConsumer(
            DSLContext ctx,
            Table<?> table,
            long bloomTermId,
            String searchTerm
    ) {
        this.ctx = ctx;
        this.table = table;
        this.bloomTermId = bloomTermId;
        this.searchTerm = searchTerm;
    }

    @Override
    public void accept(final Record record) {
        final Table<Record> categoryTable = DSL.table(DSL.name(("term_" + bloomTermId + "_" + this.table.getName())));
        final Field<?>[] insertFields = {
                DSL.field("term_id", BIGINTUNSIGNED.nullable(false)),
                DSL.field("type_id", BIGINTUNSIGNED.nullable(false)),
                DSL.field(DSL.name(categoryTable.getName(), "filter"), byte[].class)
        };
        final BloomFilterFromRecord filterFromRecord = new BloomFilterFromRecord(record, table, searchTerm);
        final Field<?>[] valueFields = {
                DSL.val(bloomTermId, ULong.class),
                DSL.val(record.getValue(BLOOMDB.FILTERTYPE.ID), ULong.class),
                DSL.val(filterFromRecord.bytes(), byte[].class)
        };
        ctx.insertInto(categoryTable).columns(insertFields).values(valueFields).execute();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || this.getClass() != object.getClass())
            return false;
        final FilterFromRecordToCategoryTableConsumer cast = (FilterFromRecordToCategoryTableConsumer) object;
        return bloomTermId == cast.bloomTermId && ctx == cast.ctx && table.equals(cast.table)
                && searchTerm.equals(cast.searchTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, table, bloomTermId, searchTerm);
    }
}
