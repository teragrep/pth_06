package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

class LatestConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        Element element = document.createElement("latest");
        element.setAttribute("value", "1000");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1000)\n" +
                ")";
        Condition elementCondition = new LatestCondition(element).condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionUpdatedTest(final Document document) {
        Element element = document.createElement("latest");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '2000-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 946677600)\n" +
                ")";
        Condition elementCondition = new LatestCondition(element).condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void equalityTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("test");
        anotherElement.setAttribute("value", "1000");
        anotherElement.setAttribute("operation", "EQUALS");
        IndexCondition eq1 = new IndexCondition(element, false);
        eq1.condition();
        IndexCondition eq2 = new IndexCondition(element, false);
        IndexCondition eq3 = new IndexCondition(element, true);
        eq3.condition();
        IndexCondition eq4 = new IndexCondition(element, true);
        IndexCondition notEq = new IndexCondition(anotherElement, false);
        IndexCondition notEq2 = new IndexCondition(element, true);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq3, eq4);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
