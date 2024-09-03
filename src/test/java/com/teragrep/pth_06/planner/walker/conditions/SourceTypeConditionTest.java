package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SourceTypeConditionTest {

    @Test
    void conditionTest() {
        String e = "\"getArchivedObjects_filter_table\".\"stream\" like 'f17'";
        String eStream = "\"streamdb\".\"stream\".\"stream\" like 'f17'";
        Condition elementCondition = new SourceTypeCondition("f17", "EQUALS", false).condition();
        Condition streamElementCondition = new SourceTypeCondition("f17", "EQUALS", true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    void negationTest() {
        String e = "not (\"getArchivedObjects_filter_table\".\"stream\" like 'f17')";
        String eStream = "not (\"streamdb\".\"stream\".\"stream\" like 'f17')";
        Condition elementCondition = new SourceTypeCondition("f17", "NOT_EQUALS", false).condition();
        Condition streamElementCondition = new SourceTypeCondition("f17", "NOT_EQUALS", true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    void equalsTest() {
        IndexCondition eq1 = new IndexCondition("946677600", "EQUALS", false);
        eq1.condition();
        IndexCondition eq2 = new IndexCondition("946677600", "EQUALS", false);
        IndexCondition eq3 = new IndexCondition("946677600", "EQUALS", true);
        eq3.condition();
        IndexCondition eq4 = new IndexCondition("946677600", "EQUALS", true);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq2, eq1);
        Assertions.assertEquals(eq3, eq4);
    }

    @Test
    void notEqualsTest() {
        IndexCondition eq1 = new IndexCondition("946677600", "EQUALS", false);
        IndexCondition notEq = new IndexCondition("1000", "EQUALS", false);
        IndexCondition notEq2 = new IndexCondition("1000", "EQUALS", true);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(notEq, eq1);
        Assertions.assertNotEquals(eq1, null);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
