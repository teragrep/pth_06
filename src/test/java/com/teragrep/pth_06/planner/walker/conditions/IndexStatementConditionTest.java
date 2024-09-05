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

import com.teragrep.blf_01.Tokenizer;
import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.DSLContext;
import org.jooq.exception.SQLDialectNotSupportedException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * Requires database setup for full test
 */
public class IndexStatementConditionTest {
    final ConditionConfig config = new ConditionConfig(DSL.using(new MockConnection(ctx -> new MockResult[0])), false, true, false);
    final Tokenizer tokenizer = new Tokenizer(32);
    @Test
    void conditionTest() {
        DSLContext ctx = DSL.using(new MockConnection(context -> new MockResult[0]));
        // only tests that database access is tried as expected
        Assertions.assertThrows(SQLDialectNotSupportedException.class, () ->
                new IndexStatementCondition(
                        "value",
                        new ConditionConfig(ctx, false, true, false),
                        tokenizer
                ).condition());
    }

    @Test
    void equalsTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", config, tokenizer);
        IndexStatementCondition eq2 = new IndexStatementCondition("946677600", config, tokenizer);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq2, eq1);
    }

    @Test
    void notEqualsTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", config, tokenizer);
        IndexStatementCondition notEq = new IndexStatementCondition("1000", config, tokenizer);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(notEq, eq1);
        Assertions.assertNotEquals(eq1, null);
    }
}
