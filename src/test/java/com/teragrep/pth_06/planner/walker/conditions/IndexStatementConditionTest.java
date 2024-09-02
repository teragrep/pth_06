package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
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


/**
 * Requires database setup for full test
 */
public class IndexStatementConditionTest {

    @Test
    @ExtendWith(DocumentExtension.class)
    void conditionTest(final Document document) {
        DSLContext ctx = DSL.using(new MockConnection(context -> new MockResult[0]));
        Element element = document.createElement("indexstatement");
        element.setAttribute("value", "192.124.0.0");
        element.setAttribute("operation", "EQUALS");
        // only tests that database access is tried as expected
        Assertions.assertThrows(SQLDialectNotSupportedException.class, () ->
                new IndexStatementCondition(
                        element,
                        new ConditionConfig(ctx, false, true, false)
                ).condition());
    }

    @Test
    @ExtendWith(DocumentExtension.class)
    void equalityTest(final Document document) {
        Element element = document.createElement("test");
        element.setAttribute("value", "946677600");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("test");
        anotherElement.setAttribute("value", "1000");
        anotherElement.setAttribute("operation", "EQUALS");
        IndexCondition eq1 = new IndexCondition(element, false);
        eq1.condition();
        IndexCondition eq2 = new IndexCondition(element, false);
        IndexCondition eq3 = new IndexCondition(element, true);
        eq3.condition();
        IndexCondition eq4 = new IndexCondition(element, true);
        IndexCondition notEq = new IndexCondition(anotherElement, false);
        IndexCondition notEq2 = new IndexCondition(element, true);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq3, eq4);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(eq1, notEq2);
        Assertions.assertNotEquals(notEq, notEq2);
    }
}
