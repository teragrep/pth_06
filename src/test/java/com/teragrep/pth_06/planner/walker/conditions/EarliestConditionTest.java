package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class EarliestConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "1000");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n" +
                ")";
        Condition elementCondition = new EarliestCondition(element).condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void equalsTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        EarliestCondition eq1 = new EarliestCondition(element);
        eq1.condition();
        EarliestCondition eq2 = new EarliestCondition(element);
        Assertions.assertEquals(eq1, eq2);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void notEqualsTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("test");
        anotherElement.setAttribute("value", "1000");
        anotherElement.setAttribute("operation", "EQUALS");
        EarliestCondition eq1 = new EarliestCondition(element);
        EarliestCondition notEq = new EarliestCondition(anotherElement);
        Assertions.assertNotEquals(eq1, notEq);
    }
}
