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

import com.teragrep.pth_06.planner.StreamDBClient;
import org.jooq.Condition;
import org.w3c.dom.Element;

import static com.teragrep.pth_06.jooq.generated.streamdb.Streamdb.STREAMDB;

public final class HostCondition implements QueryCondition {
    private final boolean streamQuery;
    private final Element element;

    public HostCondition(Element element, boolean streamQuery) {
        this.streamQuery = streamQuery;
        this.element = element;
    }

    public Condition condition() {
        final String value = element.getAttribute("value");
        final String operation = element.getAttribute("operation");
        Condition condition;
        if (streamQuery) {
            condition = STREAMDB.HOST.NAME.like(
                    value.replace('*', '%')
            );
        } else {
            condition =
                    StreamDBClient.GetArchivedObjectsFilterTable.host.like(
                            value.replace('*', '%').toLowerCase()
                    );
        }
        if (operation.equalsIgnoreCase("NOT_EQUALS")) {
            condition = condition.not();
        }
        return condition;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;
        final HostCondition cast = (HostCondition) object;
        return this.condition().toString().equals(cast.condition().toString());
    }
}