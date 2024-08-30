package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class IndexConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        Element element = document.createElement("index");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        String e = "\"getArchivedObjects_filter_table\".\"directory\" like 'f17'";
        String eStream = "\"streamdb\".\"stream\".\"directory\" like 'f17'";
        Condition elementCondition = new IndexCondition(element, false).condition();
        Condition streamElementCondition = new IndexCondition(element, true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void negationTest(final Document document) {
        Element element = document.createElement("index");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "NOT_EQUALS");
        String e = "not (\"getArchivedObjects_filter_table\".\"directory\" like 'f17')";
        String eStream = "not (\"streamdb\".\"stream\".\"directory\" like 'f17')";
        Condition elementCondition = new IndexCondition(element, false).condition();
        Condition streamElementCondition = new IndexCondition(element, true).condition();
        Assertions.assertEquals(e, elementCondition.toString());
        Assertions.assertEquals(eStream, streamElementCondition.toString());
    }
}
