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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ScanPlanImplTest {

    @Test
    public void testMergeableEnd() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan intersectingScanPlan = new ScanPlanImpl(1L, 18L, 30L, new FilterList());
        Assertions.assertTrue(scanPlan.mergeable(intersectingScanPlan));
    }

    @Test
    public void testMergeableStart() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan intersectingScanPlan = new ScanPlanImpl(1L, 18L, 30L, new FilterList());
        Assertions.assertTrue(intersectingScanPlan.mergeable(scanPlan));
    }

    @Test
    public void testDifferentStreamIDDoesNotIntersect() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan intersectingScanPlan = new ScanPlanImpl(2L, 18L, 30L, new FilterList());
        Assertions.assertFalse(intersectingScanPlan.mergeable(scanPlan));
    }

    @Test
    public void testDifferentFilterListDoesNotIntersect() {
        FilterList mustPassAll = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        FilterList mustPassOne = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, mustPassAll);
        ScanPlan intersectingScanPlan = new ScanPlanImpl(1L, 18L, 30L, mustPassOne);
        Assertions.assertFalse(scanPlan.mergeable(intersectingScanPlan));
    }

    @Test
    public void testTouchingEdgesIntersect() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan intersectingScanPlan = new ScanPlanImpl(1L, 20L, 30L, new FilterList());
        Assertions.assertTrue(intersectingScanPlan.mergeable(scanPlan));
    }

    @Test
    public void testIntersectingMerge() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan intersectingScanPlan = new ScanPlanImpl(1L, 20L, 30L, new FilterList());
        ScanPlan merged = scanPlan.merge(intersectingScanPlan);
        Assertions.assertEquals(new ScanPlanImpl(1L, 10L, 30L, new FilterList()), merged);
    }

    @Test
    public void testEncompassingMerge() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan intersectingScanPlan = new ScanPlanImpl(1L, 1L, 30L, new FilterList());
        ScanPlan merged = scanPlan.merge(intersectingScanPlan);
        Assertions.assertEquals(new ScanPlanImpl(1L, 1L, 30L, new FilterList()), merged);
    }

    @Test
    public void testRangeBetweenUpdatedEarliestUpdated() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan rangeBetween = scanPlan.toRangeBetween(15L, 20L);
        Assertions.assertFalse(rangeBetween.isStub());
        Assertions.assertEquals(15L, rangeBetween.earliest());
        Assertions.assertEquals(20L, rangeBetween.latest());
    }

    @Test
    public void testRangeBetweenUpdatedLatestUpdated() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan rangeBetween = scanPlan.toRangeBetween(10L, 15L);
        Assertions.assertFalse(rangeBetween.isStub());
        Assertions.assertEquals(10L, rangeBetween.earliest());
        Assertions.assertEquals(15L, rangeBetween.latest());
    }

    @Test
    public void testRangeBetweenUpdatedEarliestWithinBounds() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan rangeBetween = scanPlan.toRangeBetween(1L, 20L);
        Assertions.assertFalse(rangeBetween.isStub());
        Assertions.assertEquals(10L, rangeBetween.earliest());
        Assertions.assertEquals(20L, rangeBetween.latest());
    }

    @Test
    public void testRangeBetweenUpdatedLatestToEarliest() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan rangeBetween = scanPlan.toRangeBetween(10L, 10L);
        Assertions.assertTrue(rangeBetween.isStub());
    }

    @Test
    public void testRangeBetweenUpdatedLatestWithinBounds() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan rangeBetween = scanPlan.toRangeBetween(10L, 30L);
        Assertions.assertFalse(rangeBetween.isStub());
        Assertions.assertEquals(10L, rangeBetween.earliest());
        Assertions.assertEquals(20L, rangeBetween.latest());
    }

    @Test
    public void testToRangeOutsideBoundsStub() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan rangeBehind = scanPlan.toRangeBetween(1L, 9L);
        ScanPlan rangeAfter = scanPlan.toRangeBetween(21L, 50L);
        Assertions.assertTrue(rangeBehind.isStub());
        Assertions.assertTrue(rangeAfter.isStub());
    }

    @Test
    public void testFromEarliest() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan fromEarliest = scanPlan.rangeFromEarliest(15L);
        Assertions.assertEquals(15L, fromEarliest.earliest());
        Assertions.assertEquals(20L, fromEarliest.latest());
    }

    @Test
    public void testFromLatest() {
        ScanPlan scanPlan = new ScanPlanImpl(1L, 10L, 20L, new FilterList());
        ScanPlan fromEarliest = scanPlan.rangeUntilLatest(15L);
        Assertions.assertEquals(10L, fromEarliest.earliest());
        Assertions.assertEquals(15L, fromEarliest.latest());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(ScanPlanImpl.class)
                .withNonnullFields("streamId", "earliest", "latest", "filterList")
                .verify();
    }
}
