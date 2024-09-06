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

import com.teragrep.blf_01.Tokenizer;
import com.teragrep.pth_06.config.ConditionConfig;
import com.teragrep.pth_06.planner.BloomFilterTempTable;
import com.teragrep.pth_06.planner.PatternMatch;
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class IndexStatementCondition implements QueryCondition {

    private final Logger LOGGER = LoggerFactory.getLogger(IndexStatementCondition.class);

    private final String value;
    private final ConditionConfig config;
    private final Tokenizer tokenizer;
    private final Condition condition;
    private final long bloomTermId;
    private final List<Table<?>> tableList;

    public IndexStatementCondition(String value, ConditionConfig config) {
        this(value, config, new Tokenizer(0), DSL.noCondition(), 0L);
    }

    public IndexStatementCondition(String value, ConditionConfig config, Tokenizer tokenizer) {
        this(value, config, tokenizer, DSL.noCondition(), 0L);
    }

    public IndexStatementCondition(String value, ConditionConfig config, Tokenizer tokenizer, long bloomTermId) {
        this(value, config, tokenizer, DSL.noCondition(), bloomTermId);
    }

    public IndexStatementCondition(
            String value,
            ConditionConfig config,
            Tokenizer tokenizer,
            Condition condition,
            long bloomTermId
    ) {
        this.value = value;
        this.config = config;
        this.tokenizer = tokenizer;
        this.condition = condition;
        this.bloomTermId = bloomTermId;
        this.tableList = new ArrayList<>();
    }

    public Condition condition() {
        Condition newCondition = condition;
        LOGGER.info("indexstatement reached with search term <{}>", value);
        PatternMatch patternMatch = new PatternMatch(config.context(), value);
        if (tableList.isEmpty()) {
            tableList.addAll(patternMatch.toList());
        }
        if (!tableList.isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Found pattern match on <{}> table(s)", tableList.size());
            }
            Condition bloomCondition = DSL.noCondition();
            Condition noBloomCondition = DSL.noCondition();

            for (Table<?> table : tableList) {
                BloomFilterTempTable tempTable = new BloomFilterTempTable(
                        config.context(),
                        table,
                        bloomTermId,
                        patternMatch.tokenSet()
                );
                Condition tableCondition = tempTable.generateCondition();
                bloomCondition = bloomCondition.or(tableCondition);
                noBloomCondition = noBloomCondition.and(table.field("filter").isNull());
            }
            newCondition = bloomCondition.or(noBloomCondition);
        }
        return newCondition;
    }

    public List<Table<?>> matchList() {
        if (tableList.isEmpty()) {
            condition();
        }
        return tableList;
    }

    /**
     * @param object object compared against
     * @return true if object is same class and all object values are equal (tokenizer values are expected to point to
     *         same reference)
     */
    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final IndexStatementCondition cast = (IndexStatementCondition) object;
        return this.value.equals(cast.value) && this.config.equals(cast.config) && this.tokenizer == cast.tokenizer; // expects same reference
    }
}
