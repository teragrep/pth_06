package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

class LatestConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        Element element = document.createElement("latest");
        element.setAttribute("value", "1000");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '1970-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1000)\n" +
                ")";
        Condition elementCondition = new LatestCondition(element).condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionUpdatedTest(final Document document) {
        Element element = document.createElement("latest");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        String e = "(\n" +
                "  \"journaldb\".\"logfile\".\"logdate\" <= date '2000-01-01'\n" +
                "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 946677600)\n" +
                ")";
        Condition elementCondition = new LatestCondition(element).condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }
}
