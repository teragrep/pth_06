/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2024  Suomen Kanuuna Oy
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

package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.blf_01.Token;
import com.teragrep.pth_06.config.ConditionConfig;
import com.teragrep.pth_06.planner.StreamDBClient;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import com.teragrep.blf_01.Tokenizer;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;


public final class IndexStatementCondition implements QueryCondition {
    private final Logger LOGGER = LoggerFactory.getLogger(IndexStatementCondition.class);

    private final Element element;
    private final ConditionConfig config;
    private final Tokenizer tokenizer;

    public IndexStatementCondition(Element element, ConditionConfig config) {
        this(element, config, new Tokenizer(32));
    }

    public IndexStatementCondition(Element element, ConditionConfig config, Tokenizer tokenizer) {
        this.element = element;
        this.config = config;
        this.tokenizer = tokenizer;
    }

    public Condition condition() {
        final String value = element.getAttribute("value");
        final Set<Token> tokenSet = new HashSet<>(
                tokenizer.tokenize(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)))
        );

        LOGGER.info("BloomFilter tokenSet <[{}]>", tokenSet);

        final BloomFilter smallFilter = BloomFilter.create(100000, 0.01);
        final BloomFilter mediumFilter = BloomFilter.create(1000000, 0.03);
        final BloomFilter largeFilter = BloomFilter.create(2500000, 0.05);

        tokenSet.forEach(token -> {
            smallFilter.put(token.toString());
            mediumFilter.put(token.toString());
            largeFilter.put(token.toString());
        });

        final long rowId = StreamDBClient.BloomFiltersTempTable
                .insert(config.context(), smallFilter, mediumFilter, largeFilter);

        final Condition rowIdCondition = StreamDBClient.BloomFiltersTempTable.id.eq(rowId);

        final Field<byte[]> smallColumn = DSL
                .select(StreamDBClient.BloomFiltersTempTable.fe100kfp001)
                .from(StreamDBClient.BloomFiltersTempTable.BLOOM_TABLE)
                .where(rowIdCondition)
                .asField();
        final Field<byte[]> mediumColumn = DSL
                .select(StreamDBClient.BloomFiltersTempTable.fe1000kfpp003)
                .from(StreamDBClient.BloomFiltersTempTable.BLOOM_TABLE)
                .where(rowIdCondition)
                .asField();
        final Field<byte[]> largeColumn = DSL
                .select(StreamDBClient.BloomFiltersTempTable.fe2500kfpp005)
                .from(StreamDBClient.BloomFiltersTempTable.BLOOM_TABLE)
                .where(rowIdCondition)
                .asField();

        final Field<Boolean> fe100kfp001 = DSL
                .function(
                        "bloommatch", Boolean.class, smallColumn,
                        BLOOMDB.FILTER_EXPECTED_100000_FPP_001.FILTER
                );
        final Field<Boolean> fe1000kfpp003 = DSL
                .function(
                        "bloommatch", Boolean.class, mediumColumn,
                        BLOOMDB.FILTER_EXPECTED_1000000_FPP_003.FILTER
                );
        final Field<Boolean> fe2500kfpp005 = DSL
                .function(
                        "bloommatch", Boolean.class, largeColumn,
                        BLOOMDB.FILTER_EXPECTED_2500000_FPP_005.FILTER
                );

        final Condition noBloomFilter = BLOOMDB.FILTER_EXPECTED_100000_FPP_001.FILTER
                .isNull()
                .and(
                        BLOOMDB.FILTER_EXPECTED_1000000_FPP_003.FILTER
                                .isNull()
                                .and(BLOOMDB.FILTER_EXPECTED_2500000_FPP_005.FILTER.isNull())
                );
        final Condition queryCondition = fe100kfp001
                .eq(true)
                .or(fe1000kfpp003.eq(true).or(fe2500kfpp005.eq(true).or(noBloomFilter)));
        LOGGER.trace("ConditionWalker.emitElement bloomCondition part <{}>", queryCondition);

        return queryCondition;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;
        final IndexStatementCondition cast = (IndexStatementCondition) object;
        return this.condition().toString().equals(cast.condition().toString());
    }
}
