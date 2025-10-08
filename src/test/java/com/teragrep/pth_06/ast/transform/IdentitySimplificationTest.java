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

import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;
import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public final class IdentitySimplificationTest {

    @Test
    public void testLogicalWithNoChildren() {
        Expression emptyAndExpression = new AndExpression(Collections.emptyList());
        Expression emptyOrExpression = new OrExpression(Collections.emptyList());
        Expression transformedAnd = new IdentitySimplification(emptyAndExpression).transformed();
        Expression transformedOr = new IdentitySimplification(emptyOrExpression).transformed();
        Assertions.assertEquals(new EmptyExpression(), transformedAnd);
        Assertions.assertEquals(new EmptyExpression(), transformedOr);
    }

    @Test
    public void testLogicalWithSingleChild() {
        Expression value = new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.EARLIEST);
        Expression andExpression = new AndExpression(value);
        OrExpression orExpression = new OrExpression(value);
        Expression transformedAnd = new IdentitySimplification(andExpression).transformed();
        Expression transformedOr = new IdentitySimplification(orExpression).transformed();
        Assertions.assertEquals(value, transformedAnd);
        Assertions.assertEquals(value, transformedOr);
    }

    @Test
    public void testLogicalWithMoreThanOneChild() {
        Expression left = new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.EARLIEST);
        Expression right = new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.LATEST);
        Expression andExpression = new AndExpression(left, right);
        OrExpression orExpression = new OrExpression(left, right);
        Expression transformedAnd = new IdentitySimplification(andExpression).transformed();
        Expression transformedOr = new IdentitySimplification(orExpression).transformed();
        Assertions.assertEquals(andExpression, transformedAnd);
        Assertions.assertEquals(orExpression, transformedOr);
    }
}
