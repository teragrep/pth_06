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

import static org.assertj.core.api.Assertions.assertThat;


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
        element.setAttribute("value", "192.124.0.0");
        element.setAttribute("operation", "EQUALS");
        Element anotherElement = document.createElement("test");
        anotherElement.setAttribute("value", "192.124.0.1");
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
