package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.assertj.core.api.Assertions.assertThat;

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
    void equalityTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("test");
        anotherElement.setAttribute("value", "foo");
        anotherElement.setAttribute("operation", "EQUALS");
        HostCondition expected = new HostCondition(element, false);
        HostCondition actual = new HostCondition(element, false);
        actual.condition();
        HostCondition notExpected = new HostCondition(anotherElement, false);
        HostCondition notExpected2 = new HostCondition(element, true);
        Assertions.assertTrue(new QueryCondition.Smart().compare(expected.condition(), actual.condition()));
        Assertions.assertFalse(new QueryCondition.Smart().compare(expected.condition(), notExpected.condition()));
        assertThat(expected).usingRecursiveComparison().isEqualTo(actual);
        assertThat(expected.condition()).usingRecursiveComparison().isEqualTo(actual.condition());
        assertThat(expected).usingRecursiveComparison().isNotEqualTo(notExpected);
        assertThat(expected).usingRecursiveComparison().isNotEqualTo(notExpected2);
    }
}
