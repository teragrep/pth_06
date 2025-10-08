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
package com.teragrep.pth_06.ast.xml;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.LeafExpression;
import com.teragrep.pth_06.ast.LogicalExpression;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public final class OrExpression implements LogicalExpression {

    private final List<Expression> children;

    public OrExpression() {
        this(Collections.emptyList());
    }

    public OrExpression(final Expression expression) {
        this(Collections.singletonList(expression));
    }

    public OrExpression(final Expression left, final Expression right) {
        this(Arrays.asList(left, right));
    }

    public OrExpression(final List<Expression> children) {
        this.children = children;
    }

    @Override
    public Tag tag() {
        return Tag.OR;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public LeafExpression asLeaf() {
        throw new UnsupportedOperationException("asLeaf() not supported for OrExpression");
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    @Override
    public LogicalExpression asLogical() {
        return this;
    }

    @Override
    public List<Expression> children() {
        return children;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("OR(");
        for (Expression child : children) {
            sb.append(child);
            sb.append(", ");
        }
        if (!children.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length()); // remove last ", "
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final OrExpression other = (OrExpression) o;
        // equals if same children order does not matter
        return new HashSet<>(children).equals(new HashSet<>(other.children));
    }

    @Override
    public int hashCode() {
        return new HashSet<>(children).hashCode();
    }
}
