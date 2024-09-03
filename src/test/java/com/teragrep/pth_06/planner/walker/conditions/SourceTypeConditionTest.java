package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

class SourceTypeConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        String e = "\"getArchivedObjects_filter_table\".\"stream\" like 'f17'";
        String eStream = "\"streamdb\".\"stream\".\"stream\" like 'f17'";
        Condition elementCondition = new SourceTypeCondition(element, false).condition();
        Condition streamElementCondition = new SourceTypeCondition(element, true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void negationTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "NOT_EQUALS");
        String e = "not (\"getArchivedObjects_filter_table\".\"stream\" like 'f17')";
        String eStream = "not (\"streamdb\".\"stream\".\"stream\" like 'f17')";
        Condition elementCondition = new SourceTypeCondition(element, false).condition();
        Condition streamElementCondition = new SourceTypeCondition(element, true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void equalsTest(final Document document) {
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

    @Test
    @ExtendWith(DocumentExtension.class)
    void notEqualsTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("test");
        anotherElement.setAttribute("value", "1000");
        anotherElement.setAttribute("operation", "EQUALS");
        IndexCondition eq1 = new IndexCondition(element, false);
        IndexCondition notEq = new IndexCondition(anotherElement, false);
        IndexCondition notEq2 = new IndexCondition(element, true);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
