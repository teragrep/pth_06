/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2024  Suomen Kanuuna Oy
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

package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates a query condition from provided dom element
 */
public final class QueryCondition {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCondition.class);

    private final Element element;
    private final QueryConditionConfig config;
    private final Set<Table<?>> tableSet;
    private final long bloomTermId;
    private final List<Condition> conditionCache;

    private void checkValidity(Element element) {
        if (element.getTagName() == null) {
            throw new IllegalStateException("Tag name for Element was null");
        }
        if (!element.hasAttribute("operation")) {
            throw new IllegalStateException("Could not find specified or default value for 'operation' attribute from Element");
        }
        if (!element.hasAttribute("value")) {
            throw new IllegalStateException("Could not find specified or default value for 'value' attribute from Element");
        }
    }

    public QueryCondition(Element element, QueryConditionConfig config, long bloomTermId) {
        this.element = element;
        this.config = config;
        this.bloomTermId = bloomTermId;
        this.tableSet = new HashSet<>();
        this.conditionCache = new ArrayList<>(1);
    }

    public Condition condition() {
        generateCondition();
        Condition condition = conditionCache.get(0);
        LOGGER.debug("Query condition: <{}>", condition);
        return condition;
    }

    public void generateCondition() {
        if (!conditionCache.isEmpty()) {
            return;
        }
        checkValidity(element);
        String tag = element.getTagName();
        Condition condition = null;
        switch (tag.toLowerCase()) {
            case "index":
                Conditionable index = new IndexCondition(element, config.streamQuery());
                condition = index.condition();
                break;
            case "sourcetype":
                Conditionable sourceType = new SourceTypeCondition(element, config.streamQuery());
                condition = sourceType.condition();
                break;
            case "host":
                Conditionable host = new HostCondition(element, config.streamQuery());
                condition = host.condition();
                break;
        }
        if (!config.streamQuery()) {
            // Handle also time qualifiers
            if ("earliest".equalsIgnoreCase(tag) || "index_earliest".equalsIgnoreCase(tag)) {
                Conditionable earliest = new EarliestCondition(element);
                condition = earliest.condition();
            }
            if ("latest".equalsIgnoreCase(tag) || "index_latest".equalsIgnoreCase(tag)) {
                Conditionable latest = new LatestCondition(element);
                condition = latest.condition();
            }
            // value search
            if ("indexstatement".equalsIgnoreCase(tag) && config.bloomEnabled()) {
                IndexStatementCondition indexStatementCondition =
                        new IndexStatementCondition(condition, element, config, bloomTermId);
                condition = indexStatementCondition.condition();
                List<Table<?>> newMatches = indexStatementCondition.matchList();
                if (!newMatches.isEmpty()) {
                    tableSet.addAll(newMatches);
                }
            }
        }
        conditionCache.add(condition);
    }

    public boolean isIndexStatement() {
        checkValidity(element);
        String tag = element.getTagName();
        return !config.streamQuery() && "indexstatement".equalsIgnoreCase(tag) && config.bloomEnabled();
    }

    public Set<Table<?>> matchList() {
        checkValidity(element);
        generateCondition();
        return tableSet;
    }

    public long earliest() {
        checkValidity(element);
        String tag = element.getTagName();
        int earliestFromElement = Integer.MAX_VALUE;
        if ("earliest".equalsIgnoreCase(tag) || "index_earliest".equalsIgnoreCase(tag)) {
            String value = element.getAttribute("value");
            earliestFromElement = Integer.parseInt(value);

        }
        return earliestFromElement;
    }
}
