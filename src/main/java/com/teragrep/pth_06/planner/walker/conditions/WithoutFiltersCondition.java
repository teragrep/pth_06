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
package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
import com.teragrep.pth_06.planner.bloomfilter.ConditionMatchBloomDBTables;
import com.teragrep.pth_06.planner.bloomfilter.DatabaseTables;
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

public final class WithoutFiltersCondition implements QueryCondition, BloomQueryCondition {

    private final ConditionConfig config;
    private final Set<Table<?>> tables;

    public WithoutFiltersCondition(ConditionConfig config) {
        this(config, new HashSet<>());
    }

    public WithoutFiltersCondition(ConditionConfig config, Set<Table<?>> tables) {
        this.config = config;
        this.tables = new HashSet<>();
    }

    @Override
    public Condition condition() {
        final String withoutFiltersPattern = config.withoutFiltersPattern();
        final Condition condition;

        if (tables.isEmpty()) {

            final QueryCondition tableFilteringCondition = new StringEqualsCondition(
                    withoutFiltersPattern,
                    BLOOMDB.FILTERTYPE.PATTERN
            );

            final DatabaseTables conditionMatchingTables = new ConditionMatchBloomDBTables(
                    config.context(),
                    tableFilteringCondition
            );

            tables.addAll(conditionMatchingTables.tables());
        }

        if (tables.isEmpty()) {
            condition = DSL.noCondition();
        }
        else {
            Condition combinedNullFilterCondition = DSL.noCondition();
            for (final Table<?> table : tables) {
                final Condition nullFilterCondition = table.field("filter").isNull();
                combinedNullFilterCondition = combinedNullFilterCondition.and(nullFilterCondition);
            }
            condition = combinedNullFilterCondition;
        }

        return condition;
    }

    @Override
    public boolean isBloomSearchCondition() {
        return false;
    }

    @Override
    public Set<Table<?>> requiredTables() {
        if (tables.isEmpty()) {
            condition();
        }
        return tables;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final WithoutFiltersCondition that = (WithoutFiltersCondition) object;
        return Objects.equals(config, that.config) && Objects.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, tables);
    }
}
