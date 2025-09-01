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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 * 
 * @see org.jooq.QueryPart
 */
public class HostConditionTest {

    @Test
    public void conditionTest() {
        HostCondition elementCondition = new HostCondition("f17", "EQUALS", false);
        HostCondition streamElementCondition = new HostCondition("f17", "EQUALS", true);
        String e = "\"getArchivedObjects_filter_table\".\"host\" like 'f17'";
        String eStream = "\"streamdb\".\"host\".\"name\" like 'f17'";
        Condition elementResult = elementCondition.condition();
        Condition streamElementResult = streamElementCondition.condition();
        Assertions.assertEquals(e, elementResult.toString());
        Assertions.assertEquals(eStream, streamElementResult.toString());
    }

    @Test
    public void negationTest() {
        HostCondition elementCondition = new HostCondition("f17", "NOT_EQUALS", false);
        HostCondition streamElementCondition = new HostCondition("f17", "NOT_EQUALS", true);
        String e = "not (\"getArchivedObjects_filter_table\".\"host\" like 'f17')";
        String eStream = "not (\"streamdb\".\"host\".\"name\" like 'f17')";
        Condition elementResult = elementCondition.condition();
        Condition streamElementResult = streamElementCondition.condition();
        Assertions.assertEquals(e, elementResult.toString());
        Assertions.assertEquals(eStream, streamElementResult.toString());
    }

    @Test
    public void wildcardAsTrueTest() {
        HostCondition elementCondition = new HostCondition("*", "EQUALS", false);
        HostCondition streamElementCondition = new HostCondition("*", "EQUALS", true);
        Assertions.assertEquals(DSL.trueCondition(), elementCondition.condition());
        Assertions.assertEquals(DSL.trueCondition(), streamElementCondition.condition());
    }

    @Test
    public void wildcardNegationAsFalseTest() {
        HostCondition elementCondition = new HostCondition("*", "NOT_EQUALS", false);
        HostCondition streamElementCondition = new HostCondition("*", "NOT_EQUALS", true);
        Assertions.assertEquals(DSL.falseCondition(), elementCondition.condition());
        Assertions.assertEquals(DSL.falseCondition(), streamElementCondition.condition());
    }

    @Test
    public void testPartialWildcard() {
        HostCondition elementCondition = new HostCondition("value*", "EQUALS", false);
        HostCondition streamElementCondition = new HostCondition("value*", "EQUALS", true);
        String e = "\"getArchivedObjects_filter_table\".\"host\" like 'value%'";
        String eStream = "\"streamdb\".\"host\".\"name\" like 'value%'";
        Condition elementResult = elementCondition.condition();
        Condition streamElementResult = streamElementCondition.condition();
        Assertions.assertEquals(e, elementResult.toString());
        Assertions.assertEquals(eStream, streamElementResult.toString());
    }

    @Test
    public void testPartialWildcardNegation() {
        HostCondition elementCondition = new HostCondition("value*", "NOT_EQUALS", false);
        HostCondition streamElementCondition = new HostCondition("value*", "NOT_EQUALS", true);
        String e = "not (\"getArchivedObjects_filter_table\".\"host\" like 'value%')";
        String eStream = "not (\"streamdb\".\"host\".\"name\" like 'value%')";
        Condition elementResult = elementCondition.condition();
        Condition streamElementResult = streamElementCondition.condition();
        Assertions.assertEquals(e, elementResult.toString());
        Assertions.assertEquals(eStream, streamElementResult.toString());
    }

    @Test
    public void equalsTest() {
        HostCondition eq1 = new HostCondition("946677600", "EQUALS", false);
        eq1.condition();
        HostCondition eq2 = new HostCondition("946677600", "EQUALS", false);
        HostCondition eq3 = new HostCondition("946677600", "EQUALS", true);
        eq3.condition();
        HostCondition eq4 = new HostCondition("946677600", "EQUALS", true);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq3, eq4);
    }

    @Test
    public void notEqualsTest() {
        HostCondition eq1 = new HostCondition("946677600", "EQUALS", false);
        HostCondition notEq = new HostCondition("1000", "EQUALS", false);
        HostCondition notEq2 = new HostCondition("946677600", "EQUALS", true);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }

    @Test
    public void hashCodeTest() {
        HostCondition eq1 = new HostCondition("946677600", "EQUALS", false);
        HostCondition eq2 = new HostCondition("946677600", "EQUALS", false);
        HostCondition eq3 = new HostCondition("946677600", "EQUALS", true);
        HostCondition eq4 = new HostCondition("946677600", "EQUALS", true);
        HostCondition eq5 = new HostCondition("12344", "EQUALS", false);
        Assertions.assertEquals(eq1.hashCode(), eq2.hashCode());
        Assertions.assertEquals(eq3.hashCode(), eq4.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), eq4.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), eq5.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(HostCondition.class)
                .withNonnullFields("value")
                .withNonnullFields("operation")
                .withNonnullFields("streamQuery")
                .verify();
    }
}
