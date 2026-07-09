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

import com.teragrep.pth_06.ast.expressions.EarliestExpression;
import com.teragrep.pth_06.ast.expressions.EmptyExpression;
import com.teragrep.pth_06.ast.expressions.Expression;
import com.teragrep.pth_06.ast.expressions.AndExpression;
import com.teragrep.pth_06.ast.expressions.IndexExpression;
import com.teragrep.pth_06.ast.expressions.OrExpression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;

/** Tests recursive optimization results on AST */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class OptimizedASTTest {

    @Test
    public void testASTOptimization() {
        Expression indexExp = new IndexExpression("test", "equals");
        Expression earliestExp = new EarliestExpression("test_2", "equals");
        Expression ast = new AndExpression(
                new OrExpression(indexExp, indexExp),
                new AndExpression(earliestExp, earliestExp)
        );
        Expression optimizedAST = new OptimizedAST(ast).transformed();
        Expression expectedAST = new AndExpression(indexExp, earliestExp);
        Assertions.assertEquals(expectedAST, optimizedAST);
    }

    @Test
    public void testEqualBinaryMembersToValue() {
        Expression indexExp = new IndexExpression("test", "equals");
        Expression ast = new AndExpression(indexExp, indexExp);
        Expression optimizedAST = new OptimizedAST(ast).transformed();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testSingleDepthNestedBinary() {
        Expression indexExp = new IndexExpression("test", "equals");
        Expression ast = new AndExpression(new OrExpression(indexExp, indexExp), indexExp);
        Expression optimizedAST = new OptimizedAST(ast).transformed();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testMultipleDepthNestedBinaryExpressionsToValue() {
        Expression indexExp = new IndexExpression("test", "equals");
        Expression ast = new AndExpression(
                new AndExpression(new AndExpression(indexExp, indexExp), new AndExpression(indexExp, indexExp)),
                new AndExpression(indexExp, indexExp)
        );
        Expression optimizedAST = new OptimizedAST(ast).transformed();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testUnbalancedNestedBinaryExpressionsToValue() {
        Expression indexExp = new IndexExpression("test", "equals");
        Expression ast = new AndExpression(
                new AndExpression(new AndExpression(indexExp, indexExp), indexExp),
                indexExp
        );
        Expression optimizedAST = new OptimizedAST(ast).transformed();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testUnbalancedNestedEqualBinaryWithEmpty() {
        Expression indexExp = new IndexExpression("test", "equals");
        Expression ast = new AndExpression(
                new AndExpression(new AndExpression(indexExp, indexExp), new EmptyExpression()),
                indexExp
        );
        Expression optimizedAST = new OptimizedAST(ast).transformed();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testFlattenedLogical() {
        Expression value1 = new IndexExpression("1", "equals");
        Expression value2 = new IndexExpression("2", "equals");
        Expression value3 = new IndexExpression("3", "equals");
        Expression value4 = new IndexExpression("4", "equals");
        Expression value5 = new IndexExpression("5", "equals");
        Expression root = new AndExpression(
                value1,
                new AndExpression(value2, new OrExpression(value3, new OrExpression(value4, value5)))
        );
        Expression transformed = new OptimizedAST(root).transformed();
        Expression orExpr = new OrExpression(Arrays.asList(value5, value4, value3));
        Expression expected = new AndExpression(Arrays.asList(orExpr, value2, value1));
        Assertions.assertEquals(expected, transformed);
    }
}
