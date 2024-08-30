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
}
