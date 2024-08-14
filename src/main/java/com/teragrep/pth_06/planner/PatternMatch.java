/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022, 2023, 2024  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.pth_06.planner;

import com.teragrep.blf_01.Token;
import com.teragrep.blf_01.Tokenizer;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

public class PatternMatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(PatternMatch.class);

    private final DSLContext ctx;
    private final Set<Token> tokenSet;
    private final List<Table<?>> matchingTablesList;

    public PatternMatch(DSLContext ctx, String input) {
        this.ctx = ctx;
        this.tokenSet = new HashSet<>(
                new Tokenizer(0).tokenize(
                        new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))
                )
        );
        this.matchingTablesList = new ArrayList<>();
    }

    private void patternMatch() {
        Condition patternCondition = DSL.noCondition();
        for (Token token : tokenSet) {
            Field<String> tokenStringField = DSL.val(token.toString());
            patternCondition = patternCondition.or(tokenStringField.likeRegex(BLOOMDB.FILTERTYPE.PATTERN));
        }
        final Condition finalPatternCondition = patternCondition;
        List<Table<?>> tables = ctx.meta()
                .filterSchemas(s -> s.equals(BLOOMDB))
                .filterTables(t -> !t.equals(BLOOMDB.FILTERTYPE))
                .filterTables(t -> ctx.select((Field<ULong>) t.field("id"))
                        .from(t)
                        .leftJoin(BLOOMDB.FILTERTYPE)
                        .on(BLOOMDB.FILTERTYPE.ID.eq((Field<ULong>) t.field("filter_type_id")))
                        .where(finalPatternCondition)
                        .limit(1)
                        .fetch()
                        .isNotEmpty())
                .getTables();
        LOGGER.debug("Table(s) with a pattern match <{}>", tables);
        matchingTablesList.addAll(tables);
    }

    public Set<Token> tokenSet() {
        return tokenSet;
    }

    public List<Table<?>> toList() {
        if (matchingTablesList.isEmpty()) {
            patternMatch();
        }
        return matchingTablesList;
    }
}
