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

public final class StreamingCondition implements QueryCondition {

    private final String tag;
    private final String value;
    private final String operation;

    public StreamingCondition(String tag, String value, String operation) {
        this.tag = tag;
        this.value = value;
        this.operation = operation;
    }

    public Condition condition() {
        final Condition condition;
        switch (tag.toLowerCase()) {
            case "index":
                final QueryCondition index = new IndexCondition(value, operation, true);
                condition = index.condition();
                break;
            case "sourcetype":
                final QueryCondition sourceType = new SourceTypeCondition(value, operation, true);
                condition = sourceType.condition();
                break;
            case "host":
                final QueryCondition host = new HostCondition(value, operation, true);
                condition = host.condition();
                break;
            case "earliest":
            case "index_earliest":
            case "latest":
            case "index_latest":
            case "indexstatement":
                // passthrough tags return no condition
                condition = DSL.noCondition();
                break;
            default: // case when tag value is not recognized as a passthrough tag
                throw new IllegalArgumentException("Unsupported element tag <" + tag + ">");
        }
        return condition;
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
        final StreamingCondition that = (StreamingCondition) object;
        return Objects.equals(tag, that.tag) && Objects.equals(value, that.value)
                && Objects.equals(operation, that.operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, value, operation);
    }
}
