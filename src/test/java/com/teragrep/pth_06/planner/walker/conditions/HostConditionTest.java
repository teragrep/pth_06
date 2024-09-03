package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HostConditionTest {

    @Test
    void conditionTest() {
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
    void negationTest() {
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
    void equalsTest() {
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
    void notEqualsTest() {
        HostCondition eq1 = new HostCondition("946677600", "EQUALS", false);
        HostCondition notEq = new HostCondition("1000", "EQUALS", false);
        HostCondition notEq2 = new HostCondition("946677600", "EQUALS", true);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
