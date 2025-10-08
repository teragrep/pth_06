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
package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.LeafExpression;
import com.teragrep.pth_06.ast.LogicalExpression;
import com.teragrep.pth_06.ast.MergeIntersectingRanges;
import com.teragrep.pth_06.ast.meta.StreamDBCondition;
import com.teragrep.pth_06.ast.meta.StreamIDs;
import com.teragrep.pth_06.ast.transform.WithDefaultValues;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.XMLValueExpression;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class ScanGroupExpression implements LeafExpression<List<ScanRange>> {

    private final Logger LOGGER = LoggerFactory.getLogger(ScanGroupExpression.class);

    private final DSLContext ctx;
    private final List<Expression> expressions;

    public ScanGroupExpression(final DSLContext ctx, final WithDefaultValues withDefaultValues) {
        this(ctx, withDefaultValues.transformed().asLogical());
    }

    public ScanGroupExpression(final DSLContext ctx, final LogicalExpression origin) {
        this(ctx, origin.children());
    }

    public ScanGroupExpression(final DSLContext ctx, final LeafExpression origin) {
        this(ctx, new AndExpression(origin).children());
    }

    public ScanGroupExpression(final DSLContext ctx, final List<Expression> expressions) {
        this.ctx = ctx;
        this.expressions = expressions;
    }

    public List<ScanRange> value() {
        final List<XMLValueExpression> indexList = new ArrayList<>();
        final List<XMLValueExpression> hostList = new ArrayList<>();
        final List<XMLValueExpression> sourceTypeList = new ArrayList<>();
        final List<Long> earliestList = new ArrayList<>();
        final List<Long> latestList = new ArrayList<>();
        final FilterList filterList = new FilterList();

        for (final Expression child : expressions) {
            if (child.isLeaf()) {
                final Expression.Tag tag = child.tag();
                final XMLValueExpression xmlValueExpression = (XMLValueExpression) child.asLeaf();
                final String value = xmlValueExpression.value().replace("*", ".*"); // regex wildcard
                final CompareOperator operator;
                final String operation = xmlValueExpression.operation();
                if ("EQUALS".equalsIgnoreCase(operation)) {
                    operator = CompareOperator.EQUAL;
                }
                else if ("NOT_EQUALS".equalsIgnoreCase(operation)) {
                    operator = CompareOperator.NOT_EQUAL;
                }
                else {
                    throw new IllegalArgumentException("Unsupported operation <" + operation + ">");
                }
                switch (tag) {
                    case INDEX:
                        indexList.add(xmlValueExpression);
                        break;
                    case SOURCETYPE:
                        // for SQL condition
                        sourceTypeList.add(xmlValueExpression);
                        // HBase filter
                        final Filter sourceTypeFilter = new SingleColumnValueFilter(
                                Bytes.toBytes("meta"), // column family
                                Bytes.toBytes("s"), // stream
                                operator,
                                new RegexStringComparator(value)
                        );
                        filterList.addFilter(sourceTypeFilter);
                        break;
                    case HOST:
                        // for SQL condition
                        hostList.add(xmlValueExpression);
                        // HBase filter
                        final Filter hostFilter = new SingleColumnValueFilter(
                                Bytes.toBytes("meta"), // column family
                                Bytes.toBytes("h"), // host
                                operator,
                                new RegexStringComparator(value)
                        );
                        filterList.addFilter(hostFilter);
                        break;
                    case EARLIEST:
                        earliestList.add(Long.valueOf(xmlValueExpression.value()));
                        break;
                    case LATEST:
                        latestList.add(Long.valueOf(xmlValueExpression.value()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported leaf tag <" + tag + ">");
                }
            }
        }
        if (earliestList.isEmpty() || latestList.isEmpty()) {
            throw new IllegalStateException("Scan group did not have required time qualifiers");
        }
        else if ((earliestList.size() > 1 || latestList.size() > 1) && LOGGER.isWarnEnabled()) {
            LOGGER
                    .warn(
                            "Multiple time qualifiers found. earliest size: <{}> latest size: <{}>",
                            earliestList.size(), latestList.size()
                    );
        }
        final Long earliest = Collections.min(earliestList);
        final Long latest = Collections.max(latestList);

        final List<List<Long>> streamIDs = indexList
                .stream()
                // maps index expressions to StreamIDs objects
                .map(xmlExpression -> new StreamIDs(ctx, new StreamDBCondition(xmlExpression, hostList, sourceTypeList).condition())).map(StreamIDs::streamIdList).collect(Collectors.toList());
        LOGGER.info("Stream ID list <{}>", streamIDs);

        final List<ScanRange> scanRangeImpls = streamIDs
                .stream()
                // map stream ids to a list of ScanRanges
                .map(
                        streamIDList -> streamIDList
                                .stream()
                                .map(streamId -> new ScanRangeImpl(streamId, earliest, latest, filterList))
                                .collect(Collectors.toList())
                )
                // flatten resulting scan range lists into a single list
                .flatMap(List::stream)
                .collect(Collectors.toList());
        if (LOGGER.isInfoEnabled()) {
            LOGGER
                    .info(
                            "Created <{}> scan ranges with earliest <{}> to latest <{}>", scanRangeImpls.size(),
                            earliest, latest
                    );
        }
        final MergeIntersectingRanges mergeIntersectingRanges = new MergeIntersectingRanges(scanRangeImpls);
        return mergeIntersectingRanges.mergedRanges();
    }

    @Override
    public Tag tag() {
        throw new UnsupportedOperationException("tag() not supported by ScanGroupExpression");
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public LeafExpression<List<ScanRange>> asLeaf() {
        return this;
    }

    @Override
    public boolean isLogical() {
        return false;
    }

    @Override
    public LogicalExpression asLogical() {
        throw new UnsupportedOperationException("asLogical() not supported by ScanGroupExpression");
    }
}
