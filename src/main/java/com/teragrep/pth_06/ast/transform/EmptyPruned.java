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
package com.teragrep.pth_06.ast.transform;

import com.teragrep.pth_06.ast.expressions.EmptyExpression;
import com.teragrep.pth_06.ast.expressions.Expression;
import com.teragrep.pth_06.ast.expressions.AndExpression;
import com.teragrep.pth_06.ast.expressions.OrExpression;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Optimizes AND/OR expressions with empty values into value expressions
 */
public final class EmptyPruned implements ExpressionTransformation<Expression> {

    private final Expression origin;

    public EmptyPruned(final Expression origin) {
        this.origin = origin;
    }

    public Expression transformed() {
        final Expression optimizedExpression;
        if (origin.isLogical()) {
            final Expression.Tag originTag = origin.tag();
            final List<Expression> children = origin.asLogical().children();
            final EmptyExpression emptyExpression = new EmptyExpression();
            final List<Expression> nonEmptyChildren = children
                    .stream()
                    .filter(e -> !e.equals(emptyExpression))
                    .collect(Collectors.toList());
            if (originTag.equals(Expression.Tag.AND)) {
                optimizedExpression = new AndExpression(nonEmptyChildren);
            }
            else {
                optimizedExpression = new OrExpression(nonEmptyChildren);
            }
        }
        else {
            optimizedExpression = origin;
        }
        return optimizedExpression;
    }
}
