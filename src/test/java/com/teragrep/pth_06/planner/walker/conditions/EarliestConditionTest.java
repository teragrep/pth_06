package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EarliestConditionTest {

    @Test
    void conditionTest() {
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n" +
                ")";
        Condition elementCondition = new EarliestCondition("1000").condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    void equalsTest() {
        EarliestCondition eq1 = new EarliestCondition("946677600");
        eq1.condition();
        EarliestCondition eq2 = new EarliestCondition("946677600");
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq2, eq1);
    }

    @Test
    void notEqualsTest() {
        EarliestCondition eq1 = new EarliestCondition("946677600");
        EarliestCondition notEq = new EarliestCondition("1000");
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(notEq, eq1);
        Assertions.assertNotEquals(eq1, null);
    }
}
