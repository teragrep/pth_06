/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
package com.teragrep.pth_06.planner.walker;

import com.teragrep.blf_01.Token;
import com.teragrep.blf_01.Tokenizer;
import com.teragrep.pth_06.planner.StreamDBClient;
import org.apache.spark.util.sketch.BloomFilter;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;
import static com.teragrep.pth_06.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.pth_06.jooq.generated.streamdb.Streamdb.STREAMDB;

/**
 * <h1>Condition Walker</h1> Walker for conditions.
 *
 * @since 23/09/2021
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 * @author Ville Manninen
 */
public class ConditionWalker extends XmlWalker {

    private final boolean bloomEnabled;
    private final Logger LOGGER = LoggerFactory.getLogger(ConditionWalker.class);
    // Default query is full
    private boolean streamQuery = false;
    private final DSLContext ctx;

    final Tokenizer tokenizer;

    // TODO a hack to get global earliest value, default -24h from now
    private long globalEarliestEpoch = Instant.now().getEpochSecond() - 24 * 3600;

    private void updateGlobalEarliestEpoch(long earliest) {
        if (globalEarliestEpoch > earliest) {
            // decrease global earliest value
            globalEarliestEpoch = earliest;
        }
    }

    public long getGlobalEarliestEpoch() {
        return globalEarliestEpoch;
    }

    /**
     * Constructor without connection. Used during unit-tests. Enables jooq-query construction.
     */
    public ConditionWalker() {
        super();
        this.ctx = null;
        this.bloomEnabled = false;
        this.tokenizer = new Tokenizer(32);
    }

    public ConditionWalker(DSLContext ctx, boolean bloomEnabled) {
        super();
        this.ctx = ctx;
        this.bloomEnabled = bloomEnabled;
        this.tokenizer = new Tokenizer(32);
    }

    public Condition fromString(String inXml, boolean streamQuery) throws Exception {
        this.streamQuery = streamQuery;
        return fromString(inXml);
    }

    @Override
    public Condition emitLogicalOperation(String op, Object l, Object r) throws Exception {
        Condition rv;
        Condition left = (Condition) l;
        Condition right = (Condition) r;

        if (op == null) {
            throw new Exception("Parse error, unbalanced elements. " + left.toString());
        }
        if (op.equalsIgnoreCase("AND")) {
            rv = left.and(right);
        }
        else if (op.equalsIgnoreCase("OR")) {
            rv = left.or(right);
        }
        else if (op.equalsIgnoreCase("NOT")) {
            rv = left.not();
        }
        else {
            throw new Exception(
                    "Parse error, unssorted logical operation. op:" + op + " expression:" + left.toString()
            );
        }
        return rv;
    }

    @Override
    public Condition emitUnaryOperation(String op, Element current) throws Exception {

        Condition rv = emitElem(current);

        LOGGER.info("ConditionWalker.emitUnaryOperation incoming op:" + op + " element:" + current);

        if (op == null) {
            throw new Exception("Parse error, op was null");
        }
        if (rv != null) {
            if (op.equalsIgnoreCase("NOT")) {
                rv = rv.not();
            }
            else {
                throw new Exception(
                        "Parse error, unsupported logical operation. op:" + op + " expression:" + rv.toString()
                );
            }
        }
        return rv;
    }

    Condition emitElem(Element current) {

        String tag = current.getTagName();

        if (tag == null) {
            throw new IllegalArgumentException("Tag name for Element was null");
        }
        if (!current.hasAttribute("operation")) {
            throw new IllegalStateException(
                    "Could not find specified or default value for 'operation' attribute from Element"
            );
        }
        if (!current.hasAttribute("value")) {
            throw new IllegalStateException(
                    "Could not find specified or default value for 'value' attribute from Element"
            );
        }

        String value = current.getAttribute("value");
        String operation = current.getAttribute("operation");

        //System.out.println("StreamQuery="+streamQuery+" Node is terminal tag:" + tag + " val:" + value + " Operation:" + operation);
        Condition queryCondition = null;
        // directory
        if (tag.equalsIgnoreCase("index")) {
            if (streamQuery) {
                queryCondition = STREAMDB.STREAM.DIRECTORY.like(value.replace('*', '%'));
            }
            else {
                queryCondition = StreamDBClient.GetArchivedObjectsFilterTable.directory
                        .like(value.replace('*', '%').toLowerCase());
            }
            if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                queryCondition = queryCondition.not();
            }
        }
        // stream
        if (tag.equalsIgnoreCase("sourcetype")) {
            if (streamQuery) {
                queryCondition = STREAMDB.STREAM.STREAM_.like(value.replace('*', '%'));
            }
            else {
                queryCondition = StreamDBClient.GetArchivedObjectsFilterTable.stream
                        .like(value.replace('*', '%').toLowerCase());
            }
            if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                queryCondition = queryCondition.not();
            }
        }
        // host
        if (tag.equalsIgnoreCase("host")) {
            if (streamQuery) {
                queryCondition = STREAMDB.HOST.NAME.like(value.replace('*', '%'));
            }
            else {
                queryCondition = StreamDBClient.GetArchivedObjectsFilterTable.host
                        .like(value.replace('*', '%').toLowerCase());
            }
            if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                queryCondition = queryCondition.not();
            }
        }
        if (!streamQuery) {
            // Handle also time qualifiers
            if (tag.equalsIgnoreCase("earliest") || tag.equalsIgnoreCase("index_earliest")) {
                // SQL connection uses localTime in the session, so we use unix to come over the conversions
                // hour based files are being used so earliest needs conversion to the point of the last hour
                int earliestEpoch = Integer.parseInt(value);

                // TODO this is a hack to update globaol earliest value
                updateGlobalEarliestEpoch(earliestEpoch);

                int earliestEpochHour = earliestEpoch - earliestEpoch % 3600;
                Instant instant = Instant.ofEpochSecond(earliestEpochHour);
                java.sql.Date timequalifier = new Date(instant.toEpochMilli());

                queryCondition = JOURNALDB.LOGFILE.LOGDATE.greaterOrEqual(timequalifier);
                /* not supported for mariadb
                   queryCondition = queryCondition.and(toTimestamp(
                           regexpReplaceAll(JOURNALDB.LOGFILE.PATH, "((^.*\\/.*-)|(\\.log\\.gz.*))", ""),
                           "YYYYMMDDHH24").greaterOrEqual(Timestamp.from(instant)));
                   */
                // NOTE uses literal path
                queryCondition = queryCondition
                        .and(
                                "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H'))"
                                        + " >= " + instant.getEpochSecond()
                        );
            }
            if (tag.equalsIgnoreCase("latest") || tag.equalsIgnoreCase("index_latest")) {
                // SQL connection uses localTime in the session, so we use unix to come over the conversions
                Instant instant = Instant.ofEpochSecond(Integer.parseInt(value));
                java.sql.Date timequalifier = new Date(instant.toEpochMilli());

                queryCondition = JOURNALDB.LOGFILE.LOGDATE.lessOrEqual(timequalifier);
                /* not supported for mariadb
                    queryCondition = queryCondition.and(toTimestamp(
                            regexpReplaceAll(JOURNALDB.LOGFILE.PATH, "((^.*\\/.*-)|(\\.log\\.gz.*))", ""),
                            "YYYYMMDDHH24").lessOrEqual(Timestamp.from(instant)));
                */
                // NOTE uses literal path
                /*
                 to match
                 2021/09-27/sc-99-99-14-244/messages/messages-2021092722.gz.4
                 2018/04-29/sc-99-99-14-245/f17/f17.logGLOB-2018042900.log.gz
                 */

                queryCondition = queryCondition
                        .and(
                                "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H'))"
                                        + " <= " + instant.getEpochSecond()
                        );
            }
            // value search
            if ("indexstatement".equalsIgnoreCase(tag) && bloomEnabled) {
                if ("EQUALS".equals(operation)) {

                    final Set<Token> tokenSet = new HashSet<>(
                            tokenizer.tokenize(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)))
                    );

                    LOGGER.info("BloomFilter tokenSet <[{}]>", tokenSet.toString());

                    final BloomFilter smallFilter = BloomFilter.create(100000, 0.01);
                    final BloomFilter mediumFilter = BloomFilter.create(1000000, 0.03);
                    final BloomFilter largeFilter = BloomFilter.create(2500000, 0.05);

                    tokenSet.forEach(token -> {
                        smallFilter.put(token.toString());
                        mediumFilter.put(token.toString());
                        largeFilter.put(token.toString());
                    });

                    long rowId = StreamDBClient.BloomFiltersTempTable
                            .insert(ctx, smallFilter, mediumFilter, largeFilter);

                    Condition rowIdCondition = StreamDBClient.BloomFiltersTempTable.id.eq(rowId);

                    Field<byte[]> smallColumn = DSL
                            .select(StreamDBClient.BloomFiltersTempTable.fe100kfp001)
                            .from(StreamDBClient.BloomFiltersTempTable.BLOOM_TABLE)
                            .where(rowIdCondition)
                            .asField();
                    Field<byte[]> mediumColumn = DSL
                            .select(StreamDBClient.BloomFiltersTempTable.fe1000kfpp003)
                            .from(StreamDBClient.BloomFiltersTempTable.BLOOM_TABLE)
                            .where(rowIdCondition)
                            .asField();
                    Field<byte[]> largeColumn = DSL
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

                    Condition noBloomFilter = BLOOMDB.FILTER_EXPECTED_100000_FPP_001.FILTER
                            .isNull()
                            .and(
                                    BLOOMDB.FILTER_EXPECTED_1000000_FPP_003.FILTER
                                            .isNull()
                                            .and(BLOOMDB.FILTER_EXPECTED_2500000_FPP_005.FILTER.isNull())
                            );
                    queryCondition = fe100kfp001
                            .eq(true)
                            .or(fe1000kfpp003.eq(true).or(fe2500kfpp005.eq(true).or(noBloomFilter)));
                    LOGGER.trace("ConditionWalker.emitElement bloomCondition part <{}>", queryCondition);
                }
            }
        }

        return queryCondition;
    }

}
