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
package com.teragrep.pth_06.ast.expressions;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public final class OrExpressionTest {

    @Test
    public void testTag() {
        Expression.Tag tag = new OrExpression().tag();
        Assertions.assertEquals(Expression.Tag.OR, tag);
    }

    @Test
    public void testIsValue() {
        Assertions.assertFalse(new OrExpression().isValue());
    }

    @Test
    public void testIsLogical() {
        Assertions.assertTrue(new OrExpression().isLogical());
    }

    @Test
    public void testAsValue() {
        UnsupportedOperationException unsupportedOperationException = assertThrows(
                UnsupportedOperationException.class, () -> new OrExpression().asValue()
        );
        String expected = "asLeaf() not supported for OrExpression";
        Assertions.assertEquals(expected, unsupportedOperationException.getMessage());
    }

    @Test
    public void testAsLogical() {
        LogicalExpression logical = new OrExpression().asLogical();
        // test we have access to the logical expression children() method
        Assertions.assertTrue(logical.children().isEmpty());
    }

    @Test
    public void testChildren() {
        Expression value = new IndexExpression("TEST");
        Expression orExpression = new OrExpression(Arrays.asList(value, value, value));
        List<Expression> children = orExpression.asLogical().children();
        Assertions.assertFalse(children.isEmpty());
        Assertions.assertEquals(3, children.size());
        Assertions.assertTrue(children.stream().allMatch(e -> e.equals(value)));
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(OrExpression.class).withNonnullFields("children").verify();
    }
}
