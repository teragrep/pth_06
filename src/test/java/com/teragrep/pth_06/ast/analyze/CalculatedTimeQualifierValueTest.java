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

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.XMLValueExpression;
import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class CalculatedTimeQualifierValueTest {

    @Test
    public void testEarliestEquals() {
        XMLValueExpression expression = new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.EARLIEST);
        XMLValueExpression expressionLowerCase = new XMLValueExpressionImpl("1000", "equals", Expression.Tag.EARLIEST);
        CalculatedTimeQualifierValue calculated = new CalculatedTimeQualifierValue(expression);
        CalculatedTimeQualifierValue calculatedLowerCase = new CalculatedTimeQualifierValue(expressionLowerCase);
        Assertions.assertEquals(1000L, calculated.value());
        Assertions.assertEquals(1000L, calculatedLowerCase.value());
    }

    @Test
    public void testLatest() {
        XMLValueExpression expression = new XMLValueExpressionImpl("1000", "EQUALS", Expression.Tag.LATEST);
        XMLValueExpression expressionLowerCase = new XMLValueExpressionImpl("1000", "equals", Expression.Tag.LATEST);
        CalculatedTimeQualifierValue calculated = new CalculatedTimeQualifierValue(expression);
        CalculatedTimeQualifierValue calculatedLowerCase = new CalculatedTimeQualifierValue(expressionLowerCase);
        Assertions.assertEquals(1000L, calculated.value());
        Assertions.assertEquals(1000L, calculatedLowerCase.value());
    }

    @Test
    public void testGreaterThan() {
        XMLValueExpression earliestExpression = new XMLValueExpressionImpl("1000", "GE", Expression.Tag.EARLIEST);
        XMLValueExpression earliestExpressionLower = new XMLValueExpressionImpl("1000", "ge", Expression.Tag.EARLIEST);
        XMLValueExpression latestExpression = new XMLValueExpressionImpl("1000", "GE", Expression.Tag.LATEST);
        XMLValueExpression latestExpressionLower = new XMLValueExpressionImpl("1000", "ge", Expression.Tag.LATEST);
        CalculatedTimeQualifierValue calculatedEarliest = new CalculatedTimeQualifierValue(earliestExpression);
        CalculatedTimeQualifierValue calculatedEarliestLower = new CalculatedTimeQualifierValue(
                earliestExpressionLower
        );
        CalculatedTimeQualifierValue calculatedLatest = new CalculatedTimeQualifierValue(latestExpression);
        CalculatedTimeQualifierValue calculatedLatestLower = new CalculatedTimeQualifierValue(latestExpressionLower);
        Assertions.assertEquals(1001L, calculatedEarliest.value());
        Assertions.assertEquals(1001L, calculatedEarliestLower.value());
        Assertions.assertEquals(1000L, calculatedLatest.value());
        Assertions.assertEquals(1000L, calculatedLatestLower.value());
    }

    @Test
    public void testLessThan() {
        XMLValueExpression earliestExpression = new XMLValueExpressionImpl("1000", "LE", Expression.Tag.EARLIEST);
        XMLValueExpression earliestExpressionLower = new XMLValueExpressionImpl("1000", "le", Expression.Tag.EARLIEST);
        XMLValueExpression latestExpression = new XMLValueExpressionImpl("1000", "LE", Expression.Tag.LATEST);
        XMLValueExpression latestExpressionLower = new XMLValueExpressionImpl("1000", "le", Expression.Tag.LATEST);
        CalculatedTimeQualifierValue calculatedEarliest = new CalculatedTimeQualifierValue(earliestExpression);
        CalculatedTimeQualifierValue calculatedEarliestLower = new CalculatedTimeQualifierValue(
                earliestExpressionLower
        );
        CalculatedTimeQualifierValue calculatedLatest = new CalculatedTimeQualifierValue(latestExpression);
        CalculatedTimeQualifierValue calculatedLatestLower = new CalculatedTimeQualifierValue(latestExpressionLower);
        Assertions.assertEquals(1000L, calculatedEarliest.value());
        Assertions.assertEquals(1000L, calculatedEarliestLower.value());
        Assertions.assertEquals(999L, calculatedLatest.value());
        Assertions.assertEquals(999L, calculatedLatestLower.value());
    }
}
