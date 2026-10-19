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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ClassifiedXMLValueExpressionsTest {

    private final List<Expression> expressions = Arrays
            .asList(new IndexExpression("index"), new HostExpression("host"), new SourceTypeExpression("sourcetype"), new EarliestExpression("earliest"), new LatestExpression("latest"), new IndexStatementExpression("indexstatement"));

    @Test
    public void testIndexList() {
        final List<IndexExpression> indexExpressions = new ClassifiedXMLValueExpressions(expressions).indexList();
        Assertions.assertEquals(1, indexExpressions.size());
        Assertions.assertEquals("index", indexExpressions.get(0).value());
    }

    @Test
    public void testHostList() {
        final List<HostExpression> xmlValueExpressions = new ClassifiedXMLValueExpressions(expressions).hostList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("host", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testSourceTypeList() {
        final List<SourceTypeExpression> xmlValueExpressions = new ClassifiedXMLValueExpressions(expressions)
                .sourceTypeList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("sourcetype", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testEarliestList() {
        final List<EarliestExpression> xmlValueExpressions = new ClassifiedXMLValueExpressions(expressions)
                .earliestList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("earliest", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testLatestList() {
        final List<LatestExpression> xmlValueExpressions = new ClassifiedXMLValueExpressions(expressions).latestList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("latest", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testIndexStatement() {
        final List<IndexStatementExpression> xmlValueExpressions = new ClassifiedXMLValueExpressions(expressions)
                .indexStatementList();
        Assertions.assertEquals(1, xmlValueExpressions.size());
        Assertions.assertEquals("indexstatement", xmlValueExpressions.get(0).value());
    }

    @Test
    public void testEmptyList() {
        final ClassifiedXMLValueExpressions classifiedXMLValueExpressions = new ClassifiedXMLValueExpressions(
                new ArrayList<>()
        );
        Assertions.assertTrue(classifiedXMLValueExpressions.indexList().isEmpty());
        Assertions.assertTrue(classifiedXMLValueExpressions.hostList().isEmpty());
        Assertions.assertTrue(classifiedXMLValueExpressions.sourceTypeList().isEmpty());
        Assertions.assertTrue(classifiedXMLValueExpressions.earliestList().isEmpty());
        Assertions.assertTrue(classifiedXMLValueExpressions.latestList().isEmpty());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(ClassifiedXMLValueExpressions.class).withNonnullFields("expressions").verify();
    }

}
