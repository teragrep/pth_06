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
public class IndexConditionTest {
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
    void negationTest() {
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
