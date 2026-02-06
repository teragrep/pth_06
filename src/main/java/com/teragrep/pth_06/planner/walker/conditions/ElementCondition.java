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
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.w3c.dom.Element;

import java.util.Objects;
import java.util.Set;

/**
 * Creates a query condition from provided dom element
 */
public final class ElementCondition implements QueryCondition, BloomQueryCondition {

    private final ValidElement element;
    private final ConditionConfig config;

    public ElementCondition(Element element, ConditionConfig config) {
        this(new ValidElement(element), config);
    }

    public ElementCondition(ValidElement element, ConditionConfig config) {
        this.element = element;
        this.config = config;
    }

    private Condition conditionForStreamQuery() {
        final String tag = element.tag();
        final String value = element.value();
        final String operation = element.operation();
        final Condition condition;
        if ("index".equalsIgnoreCase(tag)) {
            final QueryCondition index = new IndexCondition(value, operation, true);
            condition = index.condition();
        }
        else if ("sourcetype".equalsIgnoreCase(tag)) {
            final QueryCondition sourceType = new SourceTypeCondition(value, operation, true);
            condition = sourceType.condition();
        }
        else if ("host".equalsIgnoreCase(tag)) {
            final QueryCondition host = new HostCondition(value, operation, true);
            condition = host.condition();
        }
        else {
            throw new IllegalStateException("Unsupported streaming query element tag <" + tag + ">");
        }
        return condition;
    }

    private Condition conditionForNormalQuery() {
        final String tag = element.tag();
        final String value = element.value();
        final String operation = element.operation();
        final Condition condition;
        if ("index".equalsIgnoreCase(tag)) {
            final QueryCondition index = new IndexCondition(value, operation, false);
            condition = index.condition();
        }
        else if ("sourcetype".equalsIgnoreCase(tag)) {
            final QueryCondition sourceType = new SourceTypeCondition(value, operation, false);
            condition = sourceType.condition();
        }
        else if ("host".equalsIgnoreCase(tag)) {
            final QueryCondition host = new HostCondition(value, operation, false);
            condition = host.condition();
        }
        else if ("earliest".equalsIgnoreCase(tag) || "index_earliest".equalsIgnoreCase(tag)) {
            QueryCondition earliest = new EarliestCondition(value);
            condition = earliest.condition();
        }
        else if ("latest".equalsIgnoreCase(tag) || "index_latest".equalsIgnoreCase(tag)) {
            QueryCondition latest = new LatestCondition(value);
            condition = latest.condition();
        }
        else if ("indexstatement".equalsIgnoreCase(tag) && "EQUALS".equals(operation) && config.bloomEnabled()) {
            QueryCondition indexStatement = new IndexStatementCondition(value, config);
            condition = indexStatement.condition();
        }
        else if (
            "indexstatement".equalsIgnoreCase(tag) && "EQUALS".equalsIgnoreCase(operation) && !config.bloomEnabled()
        ) {
            // ignore indexstatement if bloom is disabled
            condition = DSL.noCondition();
        }
        else {
            throw new IllegalStateException("Unsupported query element tag <" + tag + ">");
        }
        return condition;
    }

    @Override
    public Condition condition() {
        final Condition result;
        if (config.streamQuery()) {
            result = conditionForStreamQuery();
        }
        else {
            result = conditionForNormalQuery();
        }

        return result;
    }

    @Override
    public boolean isBloomSearchCondition() {
        final String tag = element.tag();
        final String operation = element.operation();
        return "indexstatement".equalsIgnoreCase(tag) && "EQUALS".equals(operation) && !config.streamQuery()
                && config.bloomEnabled();
    }

    /**
     * A set of tables needed to be joined to the query to use this condition
     */
    @Override
    public Set<Table<?>> requiredTables() {
        final String value = element.value();
        return new IndexStatementCondition(value, config).requiredTables();
    }

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
        final ElementCondition cast = (ElementCondition) object;
        return this.element.equals(cast.element) && this.config.equals(cast.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(element, config);
    }
}
