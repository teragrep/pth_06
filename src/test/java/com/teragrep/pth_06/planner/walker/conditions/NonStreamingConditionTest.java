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
package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class NonStreamingConditionTest {

    final DSLContext ctx = DSL.using(new MockConnection(ctx -> new MockResult[0]));

    @Test
    public void testValidValues() {
        final ConditionConfig config = new ConditionConfig(ctx, false);
        final List<String> validTags = Arrays
                .asList("index", "sourcetype", "host", "earliest", "index_earliest", "latest", "index_latest");
        final String value = "123456789";
        final String operation = "EQUALS";
        final List<Condition> conditions = new ArrayList<>();
        int loops = 0;
        for (final String tag : validTags) {
            final Condition result = Assertions
                    .assertDoesNotThrow(() -> new NonStreamingCondition(tag, value, operation, config).condition());
            Assertions.assertNotEquals(DSL.noCondition(), result);
            conditions.add(result);
            loops++;
        }
        Assertions.assertEquals(7, loops);
        final List<Condition> expectedResults = Collections
                .unmodifiableList(Arrays.asList(new IndexCondition(value, operation, false).condition(), new SourceTypeCondition(value, operation, false).condition(), new HostCondition(value, operation, false).condition(), new EarliestCondition(value).condition(), new EarliestCondition(value).condition(), new LatestCondition(value).condition(), new LatestCondition(value).condition()));
        Assertions.assertEquals(expectedResults, conditions);
    }

    @Test
    public void testIndexstatement() {
        // checks that tag indexstatement is recognized and a bloom search is run meaning a DataAccessException occurs
        final ConditionConfig config = new ConditionConfig(ctx, false, true);
        final String tag = "indexstatement";
        final String value = "search.value";
        final String operation = "EQUALS";
        Assertions
                .assertThrows(DataAccessException.class, () -> new NonStreamingCondition(tag, value, operation, config).condition());
    }

    @Test
    public void testUnsupportedTag() {
        final ConditionConfig config = new ConditionConfig(ctx, false);
        final String tag = "unsupported_tag";
        final String value = "search.value";
        final String operation = "EQUALS";
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> new NonStreamingCondition(tag, value, operation, config).condition()
        );
        final String expectedMessage = "Unsupported element tag <unsupported_tag>";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testEquality() {
        final ConditionConfig config = new ConditionConfig(ctx, false);
        final String tag = "index";
        final String value = "value";
        final String operation = "EQUALS";
        final NonStreamingCondition cond1 = new NonStreamingCondition(tag, value, operation, config);
        final NonStreamingCondition cond2 = new NonStreamingCondition(tag, value, operation, config);
        Assertions.assertEquals(cond1, cond2);
    }

    @Test
    public void testNonEquality() {
        final ConditionConfig config = new ConditionConfig(ctx, false);
        final ConditionConfig config2 = new ConditionConfig(ctx, false, true);
        final String tag = "index";
        final String value = "value";
        final String operation = "EQUALS";
        final NonStreamingCondition base = new NonStreamingCondition(tag, value, operation, config);
        final NonStreamingCondition cond1 = new NonStreamingCondition("indexstatement", value, operation, config);
        final NonStreamingCondition cond2 = new NonStreamingCondition(tag, "not_value", operation, config);
        final NonStreamingCondition cond3 = new NonStreamingCondition(tag, value, "NOT_EQUALS", config);
        final NonStreamingCondition cond4 = new NonStreamingCondition(tag, value, "NOT_EQUALS", config2);
        Assertions.assertNotEquals(base, cond1);
        Assertions.assertNotEquals(base, cond2);
        Assertions.assertNotEquals(base, cond3);
        Assertions.assertNotEquals(base, cond4);
    }

    @Test
    public void testHashCode() {
        final ConditionConfig config = new ConditionConfig(ctx, false);
        final ConditionConfig config2 = new ConditionConfig(ctx, false, true);
        final String tag = "index";
        final String value = "value";
        final String operation = "EQUALS";
        final NonStreamingCondition base = new NonStreamingCondition(tag, value, operation, config);
        final NonStreamingCondition base2 = new NonStreamingCondition(tag, value, operation, config);
        final NonStreamingCondition cond1 = new NonStreamingCondition("indexstatement", value, operation, config);
        final NonStreamingCondition cond2 = new NonStreamingCondition(tag, "not_value", operation, config);
        final NonStreamingCondition cond3 = new NonStreamingCondition(tag, value, "NOT_EQUALS", config);
        final NonStreamingCondition cond4 = new NonStreamingCondition(tag, value, "NOT_EQUALS", config2);
        Assertions.assertEquals(base.hashCode(), base2.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond1.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond1.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond2.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond3.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond4.hashCode());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(NonStreamingCondition.class).withNonnullFields(new String[] {
                "tag", "value", "operation", "config"
        }).verify();
    }
}
