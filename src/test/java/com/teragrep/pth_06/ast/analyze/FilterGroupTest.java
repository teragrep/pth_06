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
import com.teragrep.pth_06.ast.xml.XMLValueExpression;
import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class FilterGroupTest {

    @Test
    public void testHostFilterList() {
        List<XMLValueExpression> hostExpressions = Arrays
                .asList(new XMLValueExpressionImpl("host1", "EQUALS", Expression.Tag.HOST), new XMLValueExpressionImpl("host2", "EQUALS", Expression.Tag.HOST));
        FilterGroup filterGroup = new FilterGroup(hostExpressions, new ArrayList<>());
        FilterList filterList = filterGroup.filterList();

        List<SingleColumnValueFilter> expectedFilters = Arrays
                .asList(new SingleColumnValueFilter(Bytes.toBytes("meta"), Bytes.toBytes("h"), CompareOperator.EQUAL, new RegexStringComparator("host1")), new SingleColumnValueFilter(Bytes.toBytes("meta"), Bytes.toBytes("h"), CompareOperator.EQUAL, new RegexStringComparator("host2")));
        Assertions.assertEquals(expectedFilters, filterList.getFilters());
    }

    @Test
    public void testSourceTypeFilterList() {
        List<XMLValueExpression> sourceTypeExpressions = Arrays
                .asList(new XMLValueExpressionImpl("source1", "EQUALS", Expression.Tag.SOURCETYPE), new XMLValueExpressionImpl("source2", "EQUALS", Expression.Tag.SOURCETYPE));
        FilterGroup filterGroup = new FilterGroup(new ArrayList<>(), sourceTypeExpressions);
        FilterList filterList = filterGroup.filterList();

        List<SingleColumnValueFilter> expectedFilters = Arrays
                .asList(new SingleColumnValueFilter(Bytes.toBytes("meta"), Bytes.toBytes("s"), CompareOperator.EQUAL, new RegexStringComparator("source1")), new SingleColumnValueFilter(Bytes.toBytes("meta"), Bytes.toBytes("s"), CompareOperator.EQUAL, new RegexStringComparator("source2")));
        Assertions.assertEquals(expectedFilters, filterList.getFilters());
    }

    @Test
    public void testEmptyFilterList() {
        FilterGroup filterGroup = new FilterGroup(new ArrayList<>(), new ArrayList<>());
        FilterList filterList = filterGroup.filterList();
        Assertions.assertTrue(filterList.getFilters().isEmpty());
    }
}
