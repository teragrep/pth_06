package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 *
 * @see org.jooq.QueryPart
 */
class PatternMatchConditionTest {

    @Test
    void testSingleToken() {
        Condition condition = new PatternMatchCondition("test").condition();
        String e = "('test' like_regex \"bloomdb\".\"filtertype\".\"pattern\")";
        Assertions.assertEquals(e, condition.toString());
    }

    @Test
    void testMultipleTokens() {
        Condition condition = new PatternMatchCondition("test.nest").condition();
        String e = "(\n" +
                "  ('test.nest' like_regex \"bloomdb\".\"filtertype\".\"pattern\")\n" +
                "  or ('nest' like_regex \"bloomdb\".\"filtertype\".\"pattern\")\n" +
                "  or ('.' like_regex \"bloomdb\".\"filtertype\".\"pattern\")\n" +
                "  or ('test' like_regex \"bloomdb\".\"filtertype\".\"pattern\")\n" +
                ")";
        Assertions.assertEquals(e, condition.toString());
    }

    @Test
    void testEquality() {
        PatternMatchCondition cond1 = new PatternMatchCondition("test");
        PatternMatchCondition cond2 = new PatternMatchCondition("test");
        Assertions.assertEquals(cond1, cond2);
        cond1.condition();
        Assertions.assertEquals(cond2, cond1);
    }

    @Test
    void testNotEquals() {
        PatternMatchCondition cond1 = new PatternMatchCondition("test");
        PatternMatchCondition cond2 = new PatternMatchCondition("next");
        Assertions.assertNotEquals(cond1, cond2);
        Assertions.assertNotEquals(cond2, cond1);
    }
}