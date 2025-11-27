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
import com.teragrep.pth_06.ast.expressions.Expression;
import com.teragrep.pth_06.ast.expressions.AndExpression;
import com.teragrep.pth_06.ast.expressions.HostExpression;
import com.teragrep.pth_06.ast.expressions.IndexExpression;
import com.teragrep.pth_06.ast.expressions.LatestExpression;
import com.teragrep.pth_06.ast.expressions.OrExpression;
import com.teragrep.pth_06.ast.expressions.ValueExpression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TimeQualifiers vary slightly on compute time so only tags are asserted
 */
public final class WithDefaultValuesTest {

    @Test
    public void testAndExpressionChildren() {
        Expression input = new AndExpression(new HostExpression("host1", "equals"));
        Expression transformed = new WithDefaultValues(24L, input).transformed();
        Set<Expression.Tag> tags = transformed
                .asLogical()
                .children()
                .stream()
                .map(Expression::tag)
                .collect(Collectors.toSet());
        Set<Expression.Tag> expectedTags = new HashSet<>();
        expectedTags.add(Expression.Tag.HOST);
        expectedTags.add(Expression.Tag.INDEX);
        expectedTags.add(Expression.Tag.EARLIEST);
        expectedTags.add(Expression.Tag.LATEST);
        assertTrue(tags.containsAll(expectedTags));
    }

    @Test
    public void testOrExpressionChildren() {
        Expression value1 = new HostExpression("value1", "equals");
        Expression value2 = new IndexExpression("value2", "equals");
        Expression root = new OrExpression(Arrays.asList(value1, value2));
        Expression transformed = new WithDefaultValues(24L, root).transformed();
        Assertions.assertTrue(transformed.isLogical());
        List<Expression> orChildren = transformed.asLogical().children();
        int loops = 0;
        for (Expression child : orChildren) {
            Set<Expression.Tag> tags = new HashSet<>();
            if (child.isLogical()) {
                for (Expression c : child.asLogical().children()) {
                    tags.add(c.tag());
                }
            }
            else {
                tags.add(child.tag());
            }

            // Every child of OR should have defaults: INDEX, EARLIEST, LATEST
            Assertions.assertTrue(tags.contains(Expression.Tag.INDEX), "Child missing INDEX tag");
            Assertions.assertTrue(tags.contains(Expression.Tag.EARLIEST), "Child missing EARLIEST tag");
            Assertions.assertTrue(tags.contains(Expression.Tag.LATEST), "Child missing LATEST tag");
            loops++;
        }
        Assertions.assertEquals(2, loops);
    }

    @Test
    public void testDefaultIndexExpression() {
        Expression input = new AndExpression(new HostExpression("host1", "equals"));
        Expression transformed = new WithDefaultValues(24L, input).transformed();
        List<Expression> expressions = transformed.asLogical().children();
        Expression expectedIndex = new IndexExpression("*", "EQUALS");
        Assertions.assertEquals(4, expressions.size());
        Assertions.assertTrue(expressions.contains(expectedIndex));
    }

    @Test
    public void testDefaultEarliest() {
        long minusHours = 24;
        Expression input = new AndExpression(new HostExpression("host1", "equals"));
        Expression transformed = new WithDefaultValues(minusHours, input).transformed();
        List<Expression> expressions = transformed.asLogical().children();
        Assertions.assertEquals(4, expressions.size());
        List<ValueExpression> earliestExpressions = expressions
                .stream()
                .filter(e -> e.tag().equals(Expression.Tag.EARLIEST))
                .map(Expression::asValue)
                .collect(Collectors.toList());
        Assertions.assertEquals(1, earliestExpressions.size());
        ValueExpression expression = earliestExpressions.get(0);
        String value = expression.value();
        long earliestEpoch = Long.parseLong(value);
        // system default time zone
        long difference = Math.abs(earliestEpoch - ZonedDateTime.now().minusHours(minusHours).toEpochSecond());
        // assert that default earliest value is within 5 seconds of expected
        Assertions.assertTrue(difference < 5);
    }

    @Test
    public void testLatest() {
        Expression input = new AndExpression(new HostExpression("host1", "equals"));
        Expression transformed = new WithDefaultValues(24L, input).transformed();
        List<Expression> expressions = transformed.asLogical().children();
        Assertions.assertEquals(4, expressions.size());
        List<ValueExpression> latestExpression = expressions
                .stream()
                .filter(e -> e.tag().equals(Expression.Tag.LATEST))
                .map(Expression::asValue)
                .collect(Collectors.toList());
        Assertions.assertEquals(1, latestExpression.size());
        ValueExpression expression = latestExpression.get(0).asValue();
        String value = expression.value();
        long earliestEpoch = Long.valueOf(value);
        // system default time zone
        long difference = Math.abs(earliestEpoch - ZonedDateTime.now().toEpochSecond());
        // assert that default earliest value is within 5 seconds of expected
        Assertions.assertTrue(difference < 5);
    }

    @Test
    public void testDefaultsDoNotOverwriteExisting() {
        Expression indexExpression = new IndexExpression("value", "equals");
        Expression earliestExpression = new EarliestExpression("10000", "equals");
        Expression latestExpression = new LatestExpression("100000", "equals");
        Expression input = new AndExpression(Arrays.asList(indexExpression, earliestExpression, latestExpression));
        Expression transformed = new WithDefaultValues(24L, input).transformed();
        List<Expression> expressions = transformed.asLogical().children();
        Assertions.assertEquals(3, expressions.size());
        List<Expression> indexExpressions = expressions
                .stream()
                .filter(e -> e.tag().equals(Expression.Tag.INDEX))
                .collect(Collectors.toList());
        List<Expression> earliestExpressions = expressions
                .stream()
                .filter(e -> e.tag().equals(Expression.Tag.EARLIEST))
                .collect(Collectors.toList());
        List<Expression> latestExpressions = expressions
                .stream()
                .filter(e -> e.tag().equals(Expression.Tag.LATEST))
                .collect(Collectors.toList());
        Assertions.assertEquals(1, indexExpressions.size());
        Assertions.assertEquals(1, earliestExpressions.size());
        Assertions.assertEquals(1, latestExpressions.size());

        Assertions.assertEquals(indexExpression, indexExpressions.get(0));
        Assertions.assertEquals(earliestExpression, earliestExpressions.get(0));
        Assertions.assertEquals(latestExpression, latestExpressions.get(0));
    }
}
