package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.SQLDialectNotSupportedException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

class ElementConditionTest {
    final DSLContext mockCtx = DSL.using(new MockConnection(ctx -> new MockResult[0]));
    final ConditionConfig config = new ConditionConfig(mockCtx, false, true, false);
    final ConditionConfig streamConfig = new ConditionConfig(mockCtx, true);

    @Test
    @ExtendWith(DocumentExtension.class)
    void testStreamTags(final Document document) {
        String[] streamTags = {"index", "host", "sourcetype"};
        int loops = 0;
        for (String tag : streamTags) {
            Element element = document.createElement(tag);
            element.setAttribute("value", "1000");
            element.setAttribute("operation", "EQUALS");
            Condition condition = new ElementCondition(element, streamConfig).condition();
            Assertions.assertTrue(condition.toString().contains("1000"));
            loops++;
        }
        Assertions.assertEquals(loops, streamTags.length);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void testIndexStatement(final Document document) {
        Element element = document.createElement("indexstatement");
        element.setAttribute("value", "searchTerm");
        element.setAttribute("operation", "EQUALS");
        Element element2 = document.createElement("indexstatement");
        element2.setAttribute("value", "searchTerm");
        element2.setAttribute("operation", "NOT_EQUALS");
        Assertions.assertThrows(SQLDialectNotSupportedException.class, new ElementCondition(element, config)::condition);
        Assertions.assertThrows(IllegalStateException.class, new ElementCondition(element, streamConfig)::condition);
        Assertions.assertThrows(IllegalStateException.class, new ElementCondition(element2, config)::condition);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void testProvidedElementMissingValue(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("operation", "EQUALS");
        ElementCondition elementCondition = new ElementCondition(element, config);
        ElementCondition streamElementCondition = new ElementCondition(element, streamConfig);
        Assertions.assertThrows(IllegalStateException.class, elementCondition::condition);
        Assertions.assertThrows(IllegalStateException.class, streamElementCondition::condition);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void testProvidedElementMissingOperation(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "1000");
        ElementCondition elementCondition = new ElementCondition(element, config);
        ElementCondition streamElementCondition = new ElementCondition(element, streamConfig);
        Assertions.assertThrows(IllegalStateException.class, elementCondition::condition);
        Assertions.assertThrows(IllegalStateException.class, streamElementCondition::condition);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void testTimeQualifiers(final Document document) {
        String[] tags = {"earliest", "latest", "index_earliest", "index_latest"};
        int loops = 0;
        for (String tag : tags) {
            Element element = document.createElement(tag);
            element.setAttribute("value", "1000");
            element.setAttribute("operation", "EQUALS");
            Condition condition = new ElementCondition(element, config).condition();
            Assertions.assertTrue(condition.toString().contains("date"));
            loops++;
        }
        Assertions.assertEquals(4, loops);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void testInvalidStreamTags(final Document document) {
        String[] tags = {"earliest", "latest", "index_earliest", "index_latest", "indexstatement"};
        int loops = 0;
        for (String tag : tags) {
            Element element = document.createElement(tag);
            element.setAttribute("value", "1000");
            element.setAttribute("operation", "EQUALS");
            Assertions.assertThrows(IllegalStateException.class,
                    () -> new ElementCondition(element, streamConfig).condition()
            );
            loops++;
        }
        Assertions.assertEquals(5, loops);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void invalidElementNameTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "1000");
        element.setAttribute("operation", "EQUALS");
        Assertions.assertThrows(IllegalStateException.class,
                () -> new ElementCondition(element, streamConfig).condition()
        );
        Element element2 = document.createElement("hostindex");
        element2.setAttribute("value", "test");
        element2.setAttribute("operation", "EQUALS");
        Assertions.assertThrows(IllegalStateException.class,
                () -> new ElementCondition(element2, streamConfig).condition()
        );
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void equalsTest(final Document document) {
        Element element = document.createElement("index");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        ElementCondition eq1 = new ElementCondition(element, config);
        eq1.condition();
        ElementCondition eq2 = new ElementCondition(element, config);
        Assertions.assertEquals(eq1, eq2);
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void notEqualsTest(final Document document) {
        Element element = document.createElement("index");
        element.setAttribute("value", "f17");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("index");
        anotherElement.setAttribute("value", "f11");
        anotherElement.setAttribute("operation", "EQUALS");
        ElementCondition eq1 = new ElementCondition(element, config);
        ElementCondition notEq = new ElementCondition(anotherElement, config);
        ElementCondition notEq2 = new ElementCondition(element, streamConfig);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
