package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class EarliestConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
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
