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

import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.jooq.impl.SQLDataType.BIGINTUNSIGNED;

/**
 * Class to create a temp table of current search term and origin filter size filter types.
 * <p>
 * Origin schema:
 * <ol>
 * <li>id PK</li>
 * <li>partition_id FK journaldb.logfile.id</li>
 * <li>filter_type_id FK bloomdb.filtertype.id</li>
 * <li>filter - bloomfilter byte array</li>
 * </ol>
 * <p>
 * This schema:
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
public final class CategoryTableImpl implements CategoryTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CategoryTableImpl.class);

    private final DSLContext ctx;
    private final Table<?> originTable;
    private final long bloomTermId;
    private final TableFilters tableFilters;

    public CategoryTableImpl(ConditionConfig config, Table<?> originTable, String value) {
        this(
                config.context(),
                originTable,
                config.bloomTermId(),
                new TableFilters(config.context(), originTable, config.bloomTermId(), value)
        );
    }

    public CategoryTableImpl(DSLContext ctx, Table<?> originTable, long bloomTermId, String value) {
        this(ctx, originTable, bloomTermId, new TableFilters(ctx, originTable, bloomTermId, value));
    }

    public CategoryTableImpl(DSLContext ctx, Table<?> originTable, long bloomTermId, TableFilters tableFilters) {
        this.ctx = ctx;
        this.originTable = originTable;
        this.bloomTermId = bloomTermId;
        this.tableFilters = tableFilters;
    }

    public void create() {
        final Table<Record> namedTable = DSL.table(DSL.name(("term_" + bloomTermId + "_" + originTable.getName())));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Creating temporary table <{}>", namedTable.getName());
        }

        try (final DropTableStep dropTableStep = ctx.dropTemporaryTableIfExists(namedTable)) {
            dropTableStep.execute();
        }

        final String sql = "create temporary table " + namedTable.getName()
                + "(id bigint auto_increment primary key, term_id bigint, type_id bigint, filter longblob, unique key "
                + namedTable.getName() + "_unique_key (term_id, type_id))";
        final Query createQuery = ctx.query(sql);
        createQuery.execute();
        final Index typeIndex = DSL.index(DSL.name(namedTable.getName() + "_ix_type_id"));

        try (
                final CreateIndexIncludeStep indexStep = ctx.createIndex(typeIndex).on(namedTable, DSL.field("type_id", BIGINTUNSIGNED.nullable(false)))
        ) {
            LOGGER.trace("BloomFilterTempTable create index <{}>", indexStep);
            indexStep.execute();
        }
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
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (object.getClass() != this.getClass()) {
            return false;
        }
        final CategoryTableImpl cast = (CategoryTableImpl) object;
        return this.originTable.equals(cast.originTable) && this.ctx == cast.ctx && // equal only if same instance of DSLContext
                this.bloomTermId == cast.bloomTermId && this.tableFilters.equals(cast.tableFilters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, originTable, bloomTermId, tableFilters);
    }
}
