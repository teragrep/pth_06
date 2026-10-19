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
import com.teragrep.pth_06.ast.expressions.HostExpression;
import com.teragrep.pth_06.ast.expressions.IndexExpression;
import com.teragrep.pth_06.ast.expressions.LatestExpression;
import com.teragrep.pth_06.ast.expressions.SourceTypeExpression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public final class PrunedInvalidTimeQualifierTest {

    @Test
    public void testNonEqualsPruned() {
        Expression equalsOperation = new EarliestExpression("1000", "EQUALS");
        Expression nonEqualEarliest = new EarliestExpression("1000", "NOT_EQUALS");
        Expression nonEqualLatest = new LatestExpression("1000", "NOT_EQUALS");
        AndExpression andExpression = new AndExpression(
                Arrays.asList(equalsOperation, nonEqualEarliest, nonEqualLatest)
        );
        Expression transformed = new PrunedInvalidTimeQualifier(andExpression).transformed();
        Expression expected = new AndExpression(equalsOperation);
        Assertions.assertEquals(expected, transformed);
    }

    @Test
    public void testAllEqualsRemainsUnchanged() {
        Expression earliest = new EarliestExpression("1000", "EQUALS");
        Expression latest = new LatestExpression("1000", "EQUALS");
        Expression index = new IndexExpression("1000", "EQUALS");
        Expression host = new HostExpression("1000", "EQUALS");
        Expression sourceType = new SourceTypeExpression("1000", "EQUALS");
        Expression empty = new EmptyExpression();
        Expression andExpression = new AndExpression(Arrays.asList(earliest, latest, index, host, sourceType, empty));
        Expression transformed = new PrunedInvalidTimeQualifier(andExpression).transformed();
        Assertions.assertEquals(andExpression, transformed);
    }

    @Test
    public void testSupportedEarliestOperations() {
        Expression equals = new EarliestExpression("1000", "EQUALS");
        Expression GE = new EarliestExpression("1000", "GE");
        Expression LE = new EarliestExpression("1000", "LE");
        Expression GEQ = new EarliestExpression("1000", "GEQ");
        Expression LEQ = new EarliestExpression("1000", "LEQ");
        AndExpression andExpression = new AndExpression(Arrays.asList(equals, GE, LE, GEQ, LEQ));
        Expression pruned = new PrunedInvalidTimeQualifier(andExpression).transformed();
        Assertions.assertTrue(pruned.isLogical());
        Assertions.assertEquals(andExpression.children().size(), pruned.asLogical().children().size());
    }

    @Test
    public void testSupportedLatestOperations() {
        Expression equals = new LatestExpression("1000", "EQUALS");
        Expression GE = new LatestExpression("1000", "GE");
        Expression LE = new LatestExpression("1000", "LE");
        Expression GEQ = new LatestExpression("1000", "GEQ");
        Expression LEQ = new LatestExpression("1000", "LEQ");
        AndExpression andExpression = new AndExpression(Arrays.asList(equals, GE, LE, GEQ, LEQ));
        Expression pruned = new PrunedInvalidTimeQualifier(andExpression).transformed();
        Assertions.assertTrue(pruned.isLogical());
        Assertions.assertEquals(andExpression.children().size(), pruned.asLogical().children().size());
    }
}
