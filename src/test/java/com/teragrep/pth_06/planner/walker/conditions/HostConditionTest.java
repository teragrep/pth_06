package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HostConditionTest {
    Document document;

    @BeforeAll
    void setup() {
        Assertions.assertDoesNotThrow(() -> {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            this.document = factory.newDocumentBuilder().newDocument();
        });
    }

    @Test
    void conditionTest() {
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
    void negationTest() {
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
