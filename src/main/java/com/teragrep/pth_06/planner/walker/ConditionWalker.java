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
package com.teragrep.pth_06.planner.walker;

import com.teragrep.pth_06.config.ConditionConfig;
import com.teragrep.pth_06.planner.walker.conditions.ElementCondition;
import com.teragrep.pth_06.planner.walker.conditions.ValidElement;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * <h1>Condition Walker</h1> Walker for conditions.
 *
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 * @author Ville Manninen
 * @since 23/09/2021
 */
public final class ConditionWalker extends XmlWalker<Condition> {

    private final boolean bloomEnabled;
    private final boolean withoutFilters;
    private final Logger LOGGER = LoggerFactory.getLogger(ConditionWalker.class);
    // Default query is full
    private boolean streamQuery = false;
    private final DSLContext ctx;
    private final Set<Table<?>> combinedMatchSet;
    private long bloomTermId = 0;

    /**
     * Constructor without connection. Used during unit-tests. Enables jooq-query construction.
     */
    public ConditionWalker() {
        this(null, false, false);
    }

    public ConditionWalker(DSLContext ctx, boolean bloomEnabled) {
        this(ctx, bloomEnabled, false);
    }

    public ConditionWalker(DSLContext ctx, boolean bloomEnabled, boolean withoutFilters) {
        super();
        this.ctx = ctx;
        this.bloomEnabled = bloomEnabled;
        this.withoutFilters = withoutFilters;
        this.combinedMatchSet = new HashSet<>();
    }

    public Condition fromString(String inXml, boolean streamQueryArg)
            throws IllegalStateException, ParserConfigurationException, IOException, SAXException {
        this.streamQuery = streamQueryArg;
        return fromString(inXml);
    }

    /** Set of all tables needed to be joined to the query using the condition generated by this walker */
    public Set<Table<?>> conditionRequiredTables() {
        return combinedMatchSet;
    }

    @Override
    public Condition emitLogicalOperation(String op, Object l, Object r) throws IllegalStateException {
        Condition rv;
        Condition left = (Condition) l;
        Condition right = (Condition) r;

        if (op == null) {
            throw new IllegalStateException("Parse error, unbalanced elements. " + left.toString());
        }
        if ("AND".equalsIgnoreCase(op)) {
            rv = left.and(right);
        }
        else if ("OR".equalsIgnoreCase(op)) {
            rv = left.or(right);
        }
        else if ("NOT".equalsIgnoreCase(op)) {
            rv = left;
            if (!rv.equals(DSL.noCondition())) {
                // negate if not a noCondition
                rv = rv.not();
            }
        }
        else {
            throw new IllegalStateException(
                    "Parse error, unssorted logical operation. op:" + op + " expression:" + left.toString()
            );
        }
        return rv;
    }

    @Override
    public Condition emitUnaryOperation(String op, Element current) throws IllegalStateException {

        Condition rv = emitElem(current);

        LOGGER.info("ConditionWalker.emitUnaryOperation incoming op:" + op + " element:" + current);

        if (op == null) {
            throw new IllegalStateException("Parse error, op was null");
        }
        if (rv != null) {
            if ("NOT".equalsIgnoreCase(op)) {
                if (!rv.equals(DSL.noCondition())) {
                    // negate if not a noCondition
                    rv = rv.not();
                }
            }
            else {
                throw new IllegalStateException(
                        "Parse error, unsupported logical operation. op:" + op + " expression:" + rv.toString()
                );
            }
        }
        return rv;
    }

    Condition emitElem(final Element current) {
        final ElementCondition elementCondition = new ElementCondition(
                new ValidElement(current),
                new ConditionConfig(ctx, streamQuery, bloomEnabled, withoutFilters, bloomTermId)
        );
        if (elementCondition.isBloomSearchCondition()) {
            final Set<Table<?>> conditionRequiredTables = elementCondition.requiredTables();
            // add tables condition found to walker pattern match tables
            conditionRequiredTables().addAll(conditionRequiredTables);
            bloomTermId++;
        }
        return elementCondition.condition();
    }
}
