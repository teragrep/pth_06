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

package com.teragrep.pth_06.planner.walker;

import com.teragrep.pth_06.planner.walker.conditions.QueryCondition;
import com.teragrep.pth_06.planner.walker.conditions.QueryConditionConfig;
import org.jooq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * <h1>condition Walker</h1>
 * <p>
 * Walker for conditions.
 *
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 * @author Ville Manninen
 * @since 23/09/2021
 */
public final class ConditionWalker extends XmlWalker<Condition> {
    private final Logger LOGGER = LoggerFactory.getLogger(ConditionWalker.class);

    private final QueryConditionConfig queryConfig;
    private long bloomTermId = 0;
    private long earliestEpoch;
    private final Set<Table<?>> tableSet;

    private void updateGlobalEarliestEpoch(long newEarliest) {
        if (earliestEpoch > newEarliest) {
            earliestEpoch = newEarliest;
        }
    }

    public ConditionWalker(DSLContext ctx, boolean streamQuery) {
        super();
        this.queryConfig = new QueryConditionConfig(ctx, streamQuery, false, false);
        this.earliestEpoch = Instant.now().getEpochSecond() - 24*3600;
        this.tableSet = new HashSet<>();
    }

    public ConditionWalker(DSLContext ctx, boolean streamQuery, boolean bloomEnabled) {
        super();
        this.queryConfig = new QueryConditionConfig(ctx, streamQuery, bloomEnabled, false);
        this.earliestEpoch = Instant.now().getEpochSecond() - 24*3600;
        this.tableSet = new HashSet<>();
    }

    public ConditionWalker(DSLContext ctx, boolean streamQuery, boolean bloomEnabled, boolean bloomFilesWithoutFilter) {
        super();
        this.queryConfig = new QueryConditionConfig(ctx, streamQuery, bloomEnabled, bloomFilesWithoutFilter);
        this.earliestEpoch = Instant.now().getEpochSecond() - 24*3600;
        this.tableSet = new HashSet<>();
    }

    public Set<Table<?>> patternMatchTables() {
        return tableSet;
    }

    @Override
    public Condition emitLogicalOperation(String op, Object l, Object r) throws Exception {
        Condition rv;
        Condition left = (Condition) l;
        Condition right = (Condition) r;
        if (op == null) {
            throw new Exception("Parse error, unbalanced elements. " + left.toString());
        }
        if (op.equalsIgnoreCase("AND")) {
            rv = left.and(right);
        } else if (op.equalsIgnoreCase("OR")) {
            rv = left.or(right);
        } else if (op.equalsIgnoreCase("NOT")) {
            rv = left.not();
        } else {
            throw new Exception("Parse error, unssorted logical operation. op:" + op + " expression:" + left.toString());
        }
        return rv;
    }

    @Override
    public Condition emitUnaryOperation(String op, Element current) throws Exception {
        Condition rv = emitElem(current);
        LOGGER.info("ConditionWalker.emitUnaryOperation incoming op: <{}> element: {}", op, current);
        if (op == null) {
            throw new Exception("Parse error, op was null");
        }
        if (rv != null) {
            if (op.equalsIgnoreCase("NOT")) {
                rv = rv.not();
            } else {
                throw new Exception("Parse error, unsupported logical operation. op:" + op + " expression:" + rv);
            }
        }
        return rv;
    }

    Condition emitElem(Element current) {
        QueryCondition queryCondition = new QueryCondition(current, queryConfig, bloomTermId);
        updateGlobalEarliestEpoch(queryCondition.earliest());
        if (queryCondition.isIndexStatement()) {
            patternMatchTables().addAll(queryCondition.matchList());
            bloomTermId++;
        }
        return queryCondition.condition();
    }
}
