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
package com.teragrep.pth_06.ast;

import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;

import java.util.Collections;
import java.util.List;

/**
 * Used to format AST
 */
public final class PrintAST {

    private final Expression expression;

    public PrintAST(final Expression expression) {
        this.expression = expression;
    }

    public void print() {
        String result = printWithIndent(expression, "");
        System.out.println(result);
    }

    public String format() {
        return printWithIndent(expression, "");
    }

    private String printWithIndent(final Expression expression, final String indent) {
        Expression.Tag tag = expression.tag();
        String identIncrease = "  ";
        String result;
        List<Expression> children;
        if (expression.isLogical()) {
            children = expression.asLogical().children();
        }
        else {
            children = Collections.singletonList(expression);
        }
        switch (tag) {
            case AND:
                StringBuilder andPrint = new StringBuilder(indent + "AND\n");
                for (Expression child : children) {
                    andPrint.append(printWithIndent(child, indent + identIncrease)).append("\n");
                }
                // remove last new line
                if (andPrint.length() > 0) {
                    andPrint.setLength(andPrint.length() - 1);
                }
                result = andPrint.toString();
                break;
            case OR:
                StringBuilder orPrint = new StringBuilder(indent + "OR\n");
                for (Expression child : children) {
                    orPrint.append(printWithIndent(child, indent + identIncrease)).append("\n");
                }
                // remove last new line
                if (orPrint.length() > 0) {
                    orPrint.setLength(orPrint.length() - 1);
                }
                result = orPrint.toString();
                break;
            case INDEX:
            case SOURCETYPE:
            case HOST:
            case LATEST:
            case EARLIEST:
            case INDEXSTATEMENT:
                XMLValueExpressionImpl value = (XMLValueExpressionImpl) expression;
                result = String.format("%sVALUE%s", indent, value);
                break;
            case EMPTY:
                result = String.format("%sEMPTY", indent);
                break;
            default:
                throw new IllegalArgumentException("Unsupported tag <" + tag + ">");
        }
        return result;
    }
}
