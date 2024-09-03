package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.blf_01.Tokenizer;
import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.DSLContext;
import org.jooq.exception.SQLDialectNotSupportedException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * Requires database setup for full test
 */
public class IndexStatementConditionTest {
    final ConditionConfig config = new ConditionConfig(DSL.using(new MockConnection(ctx -> new MockResult[0])), false, true, false);
    final Tokenizer tokenizer = new Tokenizer(32);
    @Test
    void conditionTest() {
        DSLContext ctx = DSL.using(new MockConnection(context -> new MockResult[0]));
        // only tests that database access is tried as expected
        Assertions.assertThrows(SQLDialectNotSupportedException.class, () ->
                new IndexStatementCondition(
                        "value",
                        new ConditionConfig(ctx, false, true, false),
                        tokenizer
                ).condition());
    }

    @Test
    void equalsTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", config, tokenizer);
        IndexStatementCondition eq2 = new IndexStatementCondition("946677600", config, tokenizer);
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq2, eq1);
    }

    @Test
    void notEqualsTest() {
        IndexStatementCondition eq1 = new IndexStatementCondition("946677600", config, tokenizer);
        IndexStatementCondition notEq = new IndexStatementCondition("1000", config, tokenizer);
        Assertions.assertNotEquals(eq1, notEq);
        Assertions.assertNotEquals(notEq, eq1);
        Assertions.assertNotEquals(eq1, null);
    }
}
