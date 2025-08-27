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
public class SourceTypeConditionTest {

    @Test
    public void conditionTest() {
        String e = "\"getArchivedObjects_filter_table\".\"stream\" like 'f17'";
        String eStream = "\"streamdb\".\"stream\".\"stream\" like 'f17'";
        Condition elementCondition = new SourceTypeCondition("f17", "EQUALS", false).condition();
        Condition streamElementCondition = new SourceTypeCondition("f17", "EQUALS", true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    public void negationTest() {
        String e = "not (\"getArchivedObjects_filter_table\".\"stream\" like 'f17')";
        String eStream = "not (\"streamdb\".\"stream\".\"stream\" like 'f17')";
        Condition elementCondition = new SourceTypeCondition("f17", "NOT_EQUALS", false).condition();
        Condition streamElementCondition = new SourceTypeCondition("f17", "NOT_EQUALS", true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    public void wildcardAsTrueTest() {
        SourceTypeCondition elementCondition = new SourceTypeCondition("*", "EQUALS", false);
        SourceTypeCondition streamElementCondition = new SourceTypeCondition("*", "EQUALS", true);
        Assertions.assertEquals(DSL.trueCondition(), elementCondition.condition());
        Assertions.assertEquals(DSL.trueCondition(), streamElementCondition.condition());
    }

    @Test
    public void wildcardNegationAsFalseTest() {
        SourceTypeCondition elementCondition = new SourceTypeCondition("*", "NOT_EQUALS", false);
        SourceTypeCondition streamElementCondition = new SourceTypeCondition("*", "NOT_EQUALS", true);
        Assertions.assertEquals(DSL.falseCondition(), elementCondition.condition());
        Assertions.assertEquals(DSL.falseCondition(), streamElementCondition.condition());
    }

    @Test
    public void equalsTest() {
        SourceTypeCondition eq1 = new SourceTypeCondition("946677600", "EQUALS", false);
        eq1.condition();
        SourceTypeCondition eq2 = new SourceTypeCondition("946677600", "EQUALS", false);
        SourceTypeCondition eq3 = new SourceTypeCondition("946677600", "EQUALS", true);
        eq3.condition();
        SourceTypeCondition eq4 = new SourceTypeCondition("946677600", "EQUALS", true);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq3, eq4);
    }

    @Test
    public void notEqualsTest() {
        SourceTypeCondition eq1 = new SourceTypeCondition("946677600", "EQUALS", false);
        SourceTypeCondition notEq = new SourceTypeCondition("1000", "EQUALS", false);
        SourceTypeCondition notEq2 = new SourceTypeCondition("1000", "EQUALS", true);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }

    @Test
    public void hashCodeTest() {
        SourceTypeCondition eq1 = new SourceTypeCondition("946677600", "EQUALS", false);
        SourceTypeCondition eq2 = new SourceTypeCondition("946677600", "EQUALS", false);
        SourceTypeCondition notEQ1 = new SourceTypeCondition("946677600", "EQUALS", true);
        SourceTypeCondition notEQ2 = new SourceTypeCondition("1234", "EQUALS", false);
        SourceTypeCondition notEQ3 = new SourceTypeCondition("946677600", "NOT_EQUALS", false);
        Assertions.assertEquals(eq1.hashCode(), eq2.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), notEQ1.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), notEQ2.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), notEQ3.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier
                .forClass(SourceTypeCondition.class)
                .withNonnullFields("value")
                .withNonnullFields("operation")
                .withNonnullFields("streamQuery")
                .verify();
    }
}
