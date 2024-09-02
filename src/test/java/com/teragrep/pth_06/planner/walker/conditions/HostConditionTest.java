package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class HostConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        HostCondition elementCondition = new HostCondition(element, false);
        HostCondition streamElementCondition = new HostCondition(element, true);
        String e = "\"getArchivedObjects_filter_table\".\"host\" like 'f17'";
        String eStream = "\"streamdb\".\"host\".\"name\" like 'f17'";
        Condition elementResult = elementCondition.condition();
        Condition streamElementResult = streamElementCondition.condition();
        Assertions.assertEquals(e, elementResult.toString());
        Assertions.assertEquals(eStream, streamElementResult.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void negationTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "NOT_EQUALS");
        HostCondition elementCondition = new HostCondition(element, false);
        HostCondition streamElementCondition = new HostCondition(element, true);
        String e = "not (\"getArchivedObjects_filter_table\".\"host\" like 'f17')";
        String eStream = "not (\"streamdb\".\"host\".\"name\" like 'f17')";
        Condition elementResult = elementCondition.condition();
        Condition streamElementResult = streamElementCondition.condition();
        Assertions.assertEquals(e, elementResult.toString());
        Assertions.assertEquals(eStream, streamElementResult.toString());
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
        HostCondition eq1 = new HostCondition(element, false);
        eq1.condition();
        HostCondition eq2 = new HostCondition(element, false);
        HostCondition eq3 = new HostCondition(element, true);
        eq3.condition();
        HostCondition eq4 = new HostCondition(element, true);
        HostCondition notEq = new HostCondition(anotherElement, false);
        HostCondition notEq2 = new HostCondition(element, true);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq3, eq4);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
