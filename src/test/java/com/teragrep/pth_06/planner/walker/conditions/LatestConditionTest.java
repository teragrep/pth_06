package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LatestConditionTest {

    @Test
    void conditionTest() {
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1000)\n" +
                ")";
        Condition elementCondition = new LatestCondition("1000").condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    void conditionUpdatedTest() {
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '2000-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 946677600)\n" +
                ")";
        Condition elementCondition = new LatestCondition("946677600").condition();
        Assertions.assertEquals(e, elementCondition.toString());
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
        IndexCondition notEq2 = new IndexCondition("946677600", "EQUALS", true);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(notEq, eq1);
        Assertions.assertNotEquals(eq1, null);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
