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
}
