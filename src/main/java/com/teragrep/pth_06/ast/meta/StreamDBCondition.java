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
package com.teragrep.pth_06.ast.meta;

import com.teragrep.pth_06.ast.xml.XMLValueExpression;
import com.teragrep.pth_06.planner.walker.conditions.HostCondition;
import com.teragrep.pth_06.planner.walker.conditions.IndexCondition;
import com.teragrep.pth_06.planner.walker.conditions.QueryCondition;
import com.teragrep.pth_06.planner.walker.conditions.SourceTypeCondition;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class StreamDBCondition implements QueryCondition {

    private final Logger LOGGER = LoggerFactory.getLogger(StreamDBCondition.class);
    private final XMLValueExpression index;
    private final List<XMLValueExpression> hosts;
    private final List<XMLValueExpression> sourcetypes;

    public StreamDBCondition(final XMLValueExpression index) {
        this(index, Collections.emptyList(), Collections.emptyList());
    }

    public StreamDBCondition(
            final XMLValueExpression index,
            final XMLValueExpression host,
            final XMLValueExpression sourcetype
    ) {
        this(index, Collections.singletonList(host), Collections.singletonList(sourcetype));
    }

    public StreamDBCondition(
            final XMLValueExpression index,
            final List<XMLValueExpression> hosts,
            final List<XMLValueExpression> sourcetypes
    ) {
        this.index = index;
        this.hosts = hosts;
        this.sourcetypes = sourcetypes;
    }

    @Override
    public Condition condition() {
        final String value = index.value();
        final String operation = index.operation();
        LOGGER.info("Building condition for index <{}>, hosts: <{}>, sourcetypes <{}>", value, hosts, sourcetypes);
        Condition result = new IndexCondition(value, operation, true).condition();
        if (!hosts.isEmpty()) {
            result = result.and(hostCondition());
        }
        if (!sourcetypes.isEmpty()) {
            result = result.and(sourceTypeCondition());
        }
        LOGGER.info("Condition: <{}>", result);
        return result;
    }

    private Condition sourceTypeCondition() {
        Condition condition = DSL.noCondition();
        for (final XMLValueExpression sourceType : sourcetypes) {
            final String value = sourceType.value();
            final String operation = sourceType.operation();
            final Condition sourceTypeCondition = new SourceTypeCondition(value, operation, true).condition();
            if (condition == DSL.noCondition()) { // if first replace
                condition = sourceTypeCondition;
            }
            else {
                condition = condition.and(sourceTypeCondition);
            }
        }
        return condition;
    }

    private Condition hostCondition() {
        Condition condition = DSL.noCondition();
        for (final XMLValueExpression host : hosts) {
            final String value = host.value();
            final String operation = host.operation();
            final Condition hostCondition = new HostCondition(value, operation, true).condition();
            if (condition == DSL.noCondition()) { // if first replace
                condition = hostCondition;
            }
            else {
                condition = condition.and(hostCondition);
            }
        }
        return condition;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final StreamDBCondition that = (StreamDBCondition) o;
        return Objects.equals(index, that.index) && Objects.equals(hosts, that.hosts)
                && Objects.equals(sourcetypes, that.sourcetypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, hosts, sourcetypes);
    }
}
