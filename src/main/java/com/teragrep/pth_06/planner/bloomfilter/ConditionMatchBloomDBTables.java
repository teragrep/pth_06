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

import com.teragrep.pth_06.planner.walker.conditions.RegexLikeCondition;
import com.teragrep.pth_06.planner.walker.conditions.QueryCondition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

/**
 * Class to get a collection of Tables that match the given QueryCondition
 */
public final class ConditionMatchBloomDBTables implements DatabaseTables {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionMatchBloomDBTables.class);

    private final DSLContext ctx;
    private final QueryCondition condition;

    public ConditionMatchBloomDBTables(DSLContext ctx, String pattern) {
        this(ctx, new RegexLikeCondition(pattern, BLOOMDB.FILTERTYPE.PATTERN));
    }

    public ConditionMatchBloomDBTables(DSLContext ctx, QueryCondition condition) {
        this.ctx = ctx;
        this.condition = condition;
    }

    /**
     * List of tables from bloomdb that match QueryCondition Note: Table records are not fetched fully
     *
     * @return List of tables that matched QueryCondition and were not empty
     */
    public List<Table<?>> tables() {
        final List<Table<?>> tables = ctx
                .meta()
                .filterSchemas(s -> s.equals(BLOOMDB)) // select bloomdb
                .filterTables(t -> !t.equals(BLOOMDB.FILTERTYPE)) // remove filtertype table
                .filterTables(t -> ctx.select((Field<ULong>) t.field("id"))// for each remaining table
                        .from(t)
                        .leftJoin(BLOOMDB.FILTERTYPE)// join filtertype to access patterns
                        .on(BLOOMDB.FILTERTYPE.ID.eq((Field<ULong>) t.field("filter_type_id")))
                        .where(condition.condition())// select tables that match the condition
                        .limit(1)// limit 1 since we are checking only if the table is not empty
                        .fetch()
                        .isNotEmpty() // select table if not empty
                )
                .getTables();
        LOGGER.debug("Table(s) with a pattern match <{}>", tables);
        return tables;
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
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final ConditionMatchBloomDBTables cast = (ConditionMatchBloomDBTables) object;
        return this.condition.equals(cast.condition) && this.ctx == cast.ctx;
    }
}
