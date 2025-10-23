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

import com.teragrep.pth_06.ast.xml.XMLValueExpression;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public final class FilterGroup {

    private final List<XMLValueExpression> hostList;
    private final List<XMLValueExpression> sourceTypeList;

    public FilterGroup(final ClassifiedXMLValueExpressions classifiedXMLValueExpressions) {
        this(classifiedXMLValueExpressions.hostList(), classifiedXMLValueExpressions.sourceTypeList());
    }

    public FilterGroup(final List<XMLValueExpression> hostList, final List<XMLValueExpression> sourceTypeList) {
        this.hostList = hostList;
        this.sourceTypeList = sourceTypeList;
    }

    public FilterList filterList() {
        final FilterList filterList = new FilterList();

        for (XMLValueExpression hostExpression : hostList) {
            final String value = hostExpression.value();
            final String operation = hostExpression.operation();
            final CompareOperator operator;
            if ("EQUALS".equalsIgnoreCase(operation)) {
                operator = CompareOperator.EQUAL;
            }
            else if ("NOT_EQUALS".equalsIgnoreCase(operation)) {
                operator = CompareOperator.NOT_EQUAL;
            }
            else {
                throw new IllegalArgumentException("Unsupported operation <" + operation + ">");
            }
            final Filter hostFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("meta"), // column family
                    Bytes.toBytes("h"), // host
                    operator,
                    new RegexStringComparator(value)
            );
            filterList.addFilter(hostFilter);
        }

        for (final XMLValueExpression sourceTypeExpression : sourceTypeList) {
            final String value = sourceTypeExpression.value();
            final String operation = sourceTypeExpression.operation();
            final CompareOperator operator;
            if ("EQUALS".equalsIgnoreCase(operation)) {
                operator = CompareOperator.EQUAL;
            }
            else if ("NOT_EQUALS".equalsIgnoreCase(operation)) {
                operator = CompareOperator.NOT_EQUAL;
            }
            else {
                throw new IllegalArgumentException("Unsupported operation <" + operation + ">");
            }
            final Filter sourceTypeFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("meta"), // column family
                    Bytes.toBytes("s"), // stream
                    operator,
                    new RegexStringComparator(value)
            );
            filterList.addFilter(sourceTypeFilter);
        }

        return filterList;
    }
}
