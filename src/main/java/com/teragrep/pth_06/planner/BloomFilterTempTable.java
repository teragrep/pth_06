/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022, 2023, 2024  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static org.jooq.impl.SQLDataType.*;

/**
 * SQL Temp table filled with bloom filters of the parent table filter types, search term token set is
 * inserted to each filter added to the table.
 */
public class BloomFilterTempTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterTempTable.class);

    private final DSLContext ctx;
    private final Table<?> parentTable;
    private final Table<Record> tableName;
    private final long bloomTermId;
    private final Set<Token> tokenSet;
    // Table fields
    private final Field<ULong> termId;
    private final Field<ULong> typeId;
    private final Field<byte[]> filter;

    public BloomFilterTempTable(DSLContext ctx, Table<?> parentTable, long bloomtermId, Set<Token> tokenSet) {
        this.ctx = ctx;
        this.parentTable = parentTable;
        this.tableName = DSL.table(DSL.name(("term_" + bloomtermId + "_" + parentTable.getName())));
        this.bloomTermId = bloomtermId;
        this.tokenSet = tokenSet;
        this.termId = DSL.field("term_id", BIGINTUNSIGNED.nullable(false));
        this.typeId = DSL.field("type_id", BIGINTUNSIGNED.nullable(false));
        this.filter = DSL.field(DSL.name(tableName.getName(), "filter"), byte[].class);
    }

    public void create() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Creating temporary table <{}>", tableName.getName());
        }
        ctx.dropTemporaryTableIfExists(tableName).execute();
        String sql = "create temporary table " + tableName.getName() + "(id bigint auto_increment primary key, term_id bigint, type_id bigint, filter longblob, unique key " + tableName.getName() + "_unique_key (term_id, type_id))";
        Query query = ctx.query(sql);
        query.execute();
        Index logtimeIndex = DSL.index(DSL.name(tableName.getName() + "_ix_type_id"));
        CreateIndexIncludeStep createIndexIncludeStep = ctx.createIndex(logtimeIndex).on(tableName, typeId);
        LOGGER.trace("BloomFilterTempTable create index <{}>", createIndexIncludeStep);
        createIndexIncludeStep.execute();
    }

    public Condition generateCondition() {
        insertFilters();
        SelectConditionStep<Record1<byte[]>> selectConditionStep =
                DSL.select(filter)
                        .from(tableName)
                        .where(
                                termId.eq(ULong.valueOf(bloomTermId))
                        ).and(
                                typeId.eq((Field<ULong>) parentTable.field("filter_type_id"))
                        );
        Field<byte[]> termFilterColumn = selectConditionStep.asField();
        Condition filterFieldCondition = DSL.function("bloommatch", Boolean.class,
                termFilterColumn,
                parentTable.field("filter")
        ).eq(true);
        Condition notNullCondition = parentTable.field("filter").isNotNull();
        return filterFieldCondition.and(notNullCondition);
    }

    private void insertFilters() {
        Table<?> joined = parentTable;
        joined = joined.join(BLOOMDB.FILTERTYPE).on(BLOOMDB.FILTERTYPE.ID.eq(
                        (Field<ULong>) parentTable.field("filter_type_id")
                )
        );
        // Fetch filtertype values
        Result<? extends Record3<?, ?, ?>> records = ctx.select(
                        BLOOMDB.FILTERTYPE.ID,
                        joined.field("expectedElements"),
                        joined.field("targetFpp"))
                .from(joined)
                .groupBy(joined.field("filter_type_id"))
                .fetch();
        if (records.isEmpty()) {
            throw new RuntimeException("Parent table was empty");
        }
        for (Record record : records) {
            ULong filterTypeId = (ULong) record.getValue(0); // filter_type_id
            ULong expected = (ULong) record.getValue(1);  // expectedElements
            Double fpp = (Double) record.getValue(2);     // targetFpp
            BloomFilter filter = BloomFilter.create(expected.longValue(), fpp);
            tokenSet.forEach(token -> filter.put(token.toString()));

            final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
            try {
                filter.writeTo(filterBAOS);
                filterBAOS.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            ctx.insertInto(tableName).columns(
                            this.termId,
                            this.typeId,
                            this.filter
                    ).values(
                            DSL.val(bloomTermId, ULong.class),
                            DSL.val(filterTypeId, ULong.class),
                            DSL.val(filterBAOS.toByteArray(), byte[].class)
                    )
                    .execute();
        }
    }
}
