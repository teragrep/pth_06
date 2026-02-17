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

import org.jooq.Condition;
import org.jooq.impl.DSL;

import java.util.Objects;

public final class NegatedCondition implements QueryCondition {

    private final Condition origin;

    public NegatedCondition(final QueryCondition queryCondition) {
        this(queryCondition.condition());
    }

    public NegatedCondition(final Condition origin) {
        this.origin = origin;
    }

    @Override
    public org.jooq.Condition condition() {
        final Condition rv;
        if (origin.equals(DSL.trueCondition())) {
            rv = DSL.falseCondition();
        }
        else if (origin.equals(DSL.falseCondition())) {
            rv = DSL.trueCondition();
        }
        else {
            rv = origin.not();
        }
        return rv;
    }

    @Override
    public boolean equals(final Object o) {
        final boolean rv;
        if (o == null) {
            rv = false;
        }
        else if (getClass() != o.getClass()) {
            rv = false;
        }
        else {
            final NegatedCondition that = (NegatedCondition) o;
            rv = Objects.equals(origin, that.origin);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(origin);
    }
}
