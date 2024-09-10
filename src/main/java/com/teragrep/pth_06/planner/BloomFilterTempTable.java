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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static org.jooq.impl.SQLDataType.BIGINTUNSIGNED;

/**
 * Class to represent a SQL Temp. Filled with bloom filters of all the parent tables records filter types. Search term
 * token set parameter is inserted into each filter. Used to generate bloommatch condition that compares this filter
 * column against parent table filter column row by row using bloommatch UDF.
 * <p>
 * Parent Table Schema:
 * <ol>
 * <li>id PK</li>
 * <li>partition_id FK journaldb.logfile.id</li>
 * <li>filter_type_id FK bloomdb.filtertype.id</li>
 * <li>filter - bloomfilter byte array</li>
 * </ol>
 * <p>
 * Temp Table Schema:
 * <ol>
 * <li>id PK</li>
 * <li>termId - count of the current search term</li>
 * <li>typeId FK bloomdb.filtertype.id</li>
 * <li>filter - bloomfilter bytes with only the search term token set inserted</li>
 * </ol>
 * Parent table create table example:
 * <p>
 * <code>
 *     CREATE TABLE `example` ( `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `partition_id` bigint(20) unsigned NOT NULL, `filter_type_id` bigint(20) unsigned NOT NULL, `filter` longblob NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `partition_id` (`partition_id`), KEY `example_ibfk_1` (`filter_type_id`), CONSTRAINT `example_ibfk_1` FOREIGN KEY (`filter_type_id`) REFERENCES `filtertype` (`id`) ON DELETE CASCADE, CONSTRAINT `example_ibfk_2` FOREIGN KEY (`partition_id`) REFERENCES `journaldb`.`logfile` (`id`) ON DELETE CASCADE ) ENGINE=InnoDB AUTO_INCREMENT=54787 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
 * </code>
 *
 * @see com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb
 */
public final class BloomFilterTempTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterTempTable.class);

    private final DSLContext ctx;
    private final Table<?> parentTable;
    private final long bloomTermId;
    private final Set<Token> tokenSet;

    public BloomFilterTempTable(DSLContext ctx, Table<?> parentTable, long bloomTermId, Set<Token> tokenSet) {
        this.ctx = ctx;
        this.parentTable = parentTable;
        this.bloomTermId = bloomTermId;
        this.tokenSet = tokenSet;
    }

    private void create() {
        final Table<Record> table = DSL.table(DSL.name(("term_" + bloomTermId + "_" + parentTable.getName())));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Creating temporary table <{}>", table.getName());
        }
        ctx.dropTemporaryTableIfExists(table).execute();
        final String sql = "create temporary table " + table.getName()
                + "(id bigint auto_increment primary key, term_id bigint, type_id bigint, filter longblob, unique key "
                + table.getName() + "_unique_key (term_id, type_id))";
        final Query createQuery = ctx.query(sql);
        createQuery.execute();
        final Index typeIndex = DSL.index(DSL.name(table.getName() + "_ix_type_id"));
        final CreateIndexIncludeStep indexStep = ctx
                .createIndex(typeIndex)
                .on(table, DSL.field("type_id", BIGINTUNSIGNED.nullable(false)));
        LOGGER.trace("BloomFilterTempTable create index <{}>", indexStep);
        indexStep.execute();
    }

    /**
     * Inserts a filter for each filtertype inside parent table records. Filter is filled with search term token set,
     * filter size is selected using parent table filtertype expected and fpp values.
     */
    private void insertFilters() {
        final Table<?> joined = parentTable
                .join(BLOOMDB.FILTERTYPE)
                .on(BLOOMDB.FILTERTYPE.ID.eq((Field<ULong>) parentTable.field("filter_type_id")));
        final Table<Record> table = DSL.table(DSL.name(("term_" + bloomTermId + "_" + parentTable.getName())));
        final Field<ULong> expectedField = DSL.field(DSL.name(table.getName(), "expectedElements"), ULong.class);
        final Field<Double> fppField = DSL.field(DSL.name(table.getName(), "targetFpp"), Double.class);
        final SelectField<?>[] resultFields = {
                BLOOMDB.FILTERTYPE.ID,
                joined.field("expectedElements").as(expectedField),
                joined.field("targetFpp").as(fppField)
        };
        // Fetch filtertype values
        final Result<Record> records = ctx
                .select(resultFields)
                .from(joined)
                .groupBy(joined.field("filter_type_id"))
                .fetch();
        if (records.isEmpty()) {
            throw new RuntimeException("Parent table was empty");
        }
        for (Record record : records) {
            insertFilterFromRecord(record, expectedField, fppField);
        }
    }

    private void insertFilterFromRecord(
            final Record record,
            final Field<ULong> expectedField,
            final Field<Double> fppField
    ) {
        final ULong filterTypeId = record.getValue(BLOOMDB.FILTERTYPE.ID); // filter_type_id
        final ULong expected = record.getValue(expectedField); // expectedElements
        final Double fpp = record.getValue(fppField); // targetFpp
        final BloomFilter filter = BloomFilter.create(expected.longValue(), fpp);
        tokenSet.forEach(token -> filter.put(token.toString()));
        final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream();
        try {
            filter.writeTo(filterBAOS);
            filterBAOS.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(new IOException("Error writing filter bytes: " + e.getMessage()));
        }
        final Table<Record> table = DSL.table(DSL.name(("term_" + bloomTermId + "_" + parentTable.getName())));
        final Field<?>[] insertFields = {
                DSL.field("term_id", BIGINTUNSIGNED.nullable(false)),
                DSL.field("type_id", BIGINTUNSIGNED.nullable(false)),
                DSL.field(DSL.name(table.getName(), "filter"), byte[].class)
        };
        ctx
                .insertInto(table)
                .columns(insertFields)
                .values(DSL.val(bloomTermId, ULong.class), DSL.val(filterTypeId, ULong.class), DSL.val(filterBAOS.toByteArray(), byte[].class)).execute();
    }

    /**
     * Generates a condition that returns true if this temp tables search term tokens might be contained in the parent
     * table or parent table bloom filter is null. Selects to same sized filter from the temp table for each parent
     * table row.
     * <p>
     * expects the user defined function 'bloommatch' to be present to compare filter bytes. expects the parent table to
     * be joined in the query that uses the generated Condition.
     *
     * @return Condition - generated condition
     */
    public Condition generateCondition() {
        create();
        insertFilters();
        final Table<Record> table = DSL.table(DSL.name(("term_" + bloomTermId + "_" + parentTable.getName())));
        final Field<ULong> termIdField = DSL.field("term_id", BIGINTUNSIGNED.nullable(false));
        final Field<ULong> typeIdField = DSL.field("type_id", BIGINTUNSIGNED.nullable(false));
        final Field<byte[]> filterField = DSL.field(DSL.name(table.getName(), "filter"), byte[].class);
        final SelectConditionStep<Record1<byte[]>> selectFilterStep = DSL
                .select(filterField)
                .from(table)
                .where(termIdField.eq(ULong.valueOf(bloomTermId)))
                .and(typeIdField.eq((Field<ULong>) parentTable.field("filter_type_id")));
        final Field<byte[]> filterColumn = selectFilterStep.asField();
        final Condition filterFieldCondition = DSL
                .function("bloommatch", Boolean.class, filterColumn, parentTable.field("filter"))
                .eq(true);
        // null check allows SQL to optimize query
        final Condition notNullCondition = parentTable.field("filter").isNotNull();
        return filterFieldCondition.and(notNullCondition);
    }

    /**
     * Equal only if all object parameters are same value and the instances of DSLContext are same
     *
     * @param object object compared against
     * @return true if all object is same class, object fields are equal and DSLContext is same instance
     */
    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final BloomFilterTempTable cast = (BloomFilterTempTable) object;
        return this.parentTable.equals(cast.parentTable) && this.ctx == cast.ctx && // equal only if same instance of DSLContext
                this.bloomTermId == cast.bloomTermId && this.tokenSet.equals(cast.tokenSet);
    }
}
