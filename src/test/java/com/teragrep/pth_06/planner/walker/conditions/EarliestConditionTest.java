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
public class EarliestConditionTest {
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
        element.setAttribute("value", "1000");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" >= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) >= 0)\n" +
                ")";
        Condition elementCondition = new EarliestCondition(element).condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }
}
