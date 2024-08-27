package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ElementConditionTest {
    Document document;
    ConditionConfig config;
    ConditionConfig streamConfig;

    @BeforeAll
    void setup() {
        Assertions.assertDoesNotThrow(() -> {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            this.document = factory.newDocumentBuilder().newDocument();
            DSLContext mockCtx = DSL.using(new MockConnection(ctx -> new MockResult[0]));
            this.config = new ConditionConfig(mockCtx, false, true, false);
            this.streamConfig = new ConditionConfig(mockCtx, true);
        });
    }

    @Test
    void testStreamTags() {
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
    void testTimeQualifiers() {
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
        Assertions.assertEquals(loops, tags.length);
    }

    @Test
    void testInvalidStreamTags() {
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
        Assertions.assertEquals(loops, tags.length);
    }
}