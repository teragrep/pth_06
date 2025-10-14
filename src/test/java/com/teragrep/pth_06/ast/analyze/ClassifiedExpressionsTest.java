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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ClassifiedExpressionsTest {

    private final List<Expression> expressions = Arrays
            .asList(new XMLValueExpressionImpl("index", "EQUALS", Expression.Tag.INDEX), new XMLValueExpressionImpl("host", "EQUALS", Expression.Tag.HOST), new XMLValueExpressionImpl("sourcetype", "EQUALS", Expression.Tag.SOURCETYPE), new XMLValueExpressionImpl("earliest", "EQUALS", Expression.Tag.EARLIEST), new XMLValueExpressionImpl("latest", "EQUALS", Expression.Tag.LATEST));

    @Test
    public void testIndexList() {
        List<XMLValueExpression> xmlValueExpressions = new ClassifiedExpressions(expressions).indexList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("index", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testHostList() {
        List<XMLValueExpression> xmlValueExpressions = new ClassifiedExpressions(expressions).hostList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("host", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testSourceTypeList() {
        List<XMLValueExpression> xmlValueExpressions = new ClassifiedExpressions(expressions).sourceTypeList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("sourcetype", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testEarliestList() {
        List<XMLValueExpression> xmlValueExpressions = new ClassifiedExpressions(expressions).earliestList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("earliest", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testLatestList() {
        List<XMLValueExpression> xmlValueExpressions = new ClassifiedExpressions(expressions).latestList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("latest", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testEmptyList() {
        ClassifiedExpressions classifiedExpressions = new ClassifiedExpressions(new ArrayList<>());
        Assertions.assertTrue(classifiedExpressions.indexList().isEmpty());
        Assertions.assertTrue(classifiedExpressions.hostList().isEmpty());
        Assertions.assertTrue(classifiedExpressions.sourceTypeList().isEmpty());
        Assertions.assertTrue(classifiedExpressions.earliestList().isEmpty());
        Assertions.assertTrue(classifiedExpressions.latestList().isEmpty());
    }

}
