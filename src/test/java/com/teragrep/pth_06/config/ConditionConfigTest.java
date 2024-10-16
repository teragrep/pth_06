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
package com.teragrep.pth_06.config;

import com.teragrep.pth_06.planner.walker.conditions.SourceTypeCondition;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConditionConfigTest {

    DSLContext ctx = DSL.using(new MockConnection(c -> new MockResult[0]));

    @Test
    void testEquality() {
        ConditionConfig cond1 = new ConditionConfig(ctx, false, false, 1L);
        ConditionConfig cond2 = new ConditionConfig(ctx, false, false, 1L);
        Assertions.assertEquals(cond1, cond2);
    }

    @Test
    void testNonEquality() {
        ConditionConfig cond1 = new ConditionConfig(ctx, false, false);
        ConditionConfig cond2 = new ConditionConfig(ctx, true, false);
        ConditionConfig cond3 = new ConditionConfig(ctx, false, true);
        ConditionConfig cond4 = new ConditionConfig(ctx, false, true , 1L);
        Assertions.assertNotEquals(cond1, cond2);
        Assertions.assertNotEquals(cond1, cond3);
        Assertions.assertNotEquals(cond1, cond4);
    }

    @Test
    void testHashCode() {
        ConditionConfig cond1 = new ConditionConfig(ctx, false, false);
        ConditionConfig cond2 = new ConditionConfig(ctx, false, false);
        ConditionConfig cond3 = new ConditionConfig(ctx, true, false);
        ConditionConfig cond4 = new ConditionConfig(ctx, false, true);
        ConditionConfig cond5 = new ConditionConfig(ctx, false, false, 1L);
        Assertions.assertEquals(cond1.hashCode(), cond2.hashCode());
        Assertions.assertNotEquals(cond1.hashCode(), cond3.hashCode());
        Assertions.assertNotEquals(cond1.hashCode(), cond4.hashCode());
        Assertions.assertNotEquals(cond1.hashCode(), cond5.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier.forClass(ConditionConfig.class).verify();
    }
}
