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
package com.teragrep.pth_06.ast.meta;

import com.teragrep.pth_06.ast.expressions.HostExpression;
import com.teragrep.pth_06.ast.expressions.IndexExpression;
import com.teragrep.pth_06.ast.expressions.SourceTypeExpression;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class StreamDBConditionTest {

    @Test
    public void testIndexEquals() {
        Condition streamDBCondition = new StreamDBCondition(new IndexExpression("index")).condition();
        String expected = "\"streamdb\".\"stream\".\"directory\" like 'index'";
        Assertions.assertEquals(expected, streamDBCondition.toString());
    }

    @Test
    public void testIndexWildcard() {
        Condition streamDBCondition = new StreamDBCondition(new IndexExpression("*")).condition();
        String expected = "\"streamdb\".\"stream\".\"directory\" like '%'";
        Assertions.assertEquals(expected, streamDBCondition.toString());
    }

    @Test
    public void testIndexNotEquals() {
        Condition streamDBCondition = new StreamDBCondition(new IndexExpression("index", "NOT_EQUALS")).condition();
        String expected = "not (\"streamdb\".\"stream\".\"directory\" like 'index')";
        Assertions.assertEquals(expected, streamDBCondition.toString());
    }

    @Test
    public void testHost() {
        List<HostExpression> hostExpressions = Arrays
                .asList(new HostExpression("host_1"), new HostExpression("host_2"));
        Condition streamDBCondition = new StreamDBCondition(
                new IndexExpression("index"),
                hostExpressions,
                Collections.emptyList()
        ).condition();
        String expected = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'index'\n"
                + "  and \"streamdb\".\"host\".\"name\" like 'host_1'\n"
                + "  and \"streamdb\".\"host\".\"name\" like 'host_2'\n" + ")";
        Assertions.assertEquals(expected, streamDBCondition.toString());
    }

    @Test
    public void testSourcetype() {
        List<SourceTypeExpression> sourceTypeExpressions = Arrays
                .asList(new SourceTypeExpression("source_1"), new SourceTypeExpression("source_2"));
        Condition streamDBCondition = new StreamDBCondition(
                new IndexExpression("index"),
                Collections.emptyList(),
                sourceTypeExpressions
        ).condition();
        String expected = "(\n" + "  \"streamdb\".\"stream\".\"directory\" like 'index'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'source_1'\n"
                + "  and \"streamdb\".\"stream\".\"stream\" like 'source_2'\n" + ")";
        Assertions.assertEquals(expected, streamDBCondition.toString());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(StreamDBCondition.class)
                .withNonnullFields("index", "hosts", "sourcetypes")
                .withIgnoredFields("LOGGER")
                .verify();
    }
}
