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
package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.expressions.EarliestExpression;
import com.teragrep.pth_06.ast.expressions.Expression;
import com.teragrep.pth_06.ast.expressions.HostExpression;
import com.teragrep.pth_06.ast.expressions.IndexExpression;
import com.teragrep.pth_06.ast.expressions.IndexStatementExpression;
import com.teragrep.pth_06.ast.expressions.LatestExpression;
import com.teragrep.pth_06.ast.expressions.SourceTypeExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ClassifiedXMLValueExpressions {

    private final List<Expression> expressions;

    public ClassifiedXMLValueExpressions(final List<Expression> expressions) {
        this.expressions = expressions;
    }

    public List<IndexExpression> indexList() {
        final List<IndexExpression> list = new ArrayList<>();
        for (final Expression expression : expressions) {
            if (expression.tag().equals(Expression.Tag.INDEX) && expression.isValue()) {
                list.add((IndexExpression) expression.asValue());
            }
        }
        return list;
    }

    public List<SourceTypeExpression> sourceTypeList() {
        final List<SourceTypeExpression> list = new ArrayList<>();
        for (final Expression expression : expressions) {
            if (expression.tag().equals(Expression.Tag.SOURCETYPE) && expression.isValue()) {
                list.add((SourceTypeExpression) expression.asValue());
            }
        }
        return list;
    }

    public List<HostExpression> hostList() {
        final List<HostExpression> list = new ArrayList<>();
        for (final Expression expression : expressions) {
            if (expression.tag().equals(Expression.Tag.HOST) && expression.isValue()) {
                list.add((HostExpression) expression.asValue());
            }
        }
        return list;
    }

    public List<EarliestExpression> earliestList() {
        final List<EarliestExpression> list = new ArrayList<>();
        for (final Expression expression : expressions) {
            if (expression.tag().equals(Expression.Tag.EARLIEST) && expression.isValue()) {
                list.add((EarliestExpression) expression.asValue());
            }
        }
        return list;
    }

    public List<LatestExpression> latestList() {
        final List<LatestExpression> list = new ArrayList<>();
        for (final Expression expression : expressions) {
            if (expression.tag().equals(Expression.Tag.LATEST) && expression.isValue()) {
                list.add((LatestExpression) expression.asValue());
            }
        }
        return list;
    }

    public List<IndexStatementExpression> indexStatementList() {
        final List<IndexStatementExpression> list = new ArrayList<>();
        for (final Expression expression : expressions) {
            if (expression.tag().equals(Expression.Tag.INDEXSTATEMENT) && expression.isValue()) {
                list.add((IndexStatementExpression) expression.asValue());
            }
        }
        return list;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final ClassifiedXMLValueExpressions that = (ClassifiedXMLValueExpressions) o;
        return Objects.equals(expressions, that.expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expressions);
    }
}
