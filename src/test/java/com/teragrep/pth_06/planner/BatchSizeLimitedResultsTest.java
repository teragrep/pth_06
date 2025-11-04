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
package com.teragrep.pth_06.planner;

import com.codahale.metrics.MetricRegistry;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.fakes.HourlySlicesFake;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class BatchSizeLimitedResultsTest {

    final Map<String, String> opts = new HashMap<>();

    @BeforeAll
    public void setup() {
        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("queryXML", "query");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", "username");
        opts.put("DBpassword", "password");
        opts.put("DBurl", "url");
    }

    @Test
    public void testStartingOffset() {
        long startingOffset = Long.MIN_VALUE;
        BatchSizeLimit batchSizeLimit = new BatchSizeLimit(10000, 2);
        BatchSizeLimitedResults batchSizeLimitedResults = new BatchSizeLimitedResults(
                new HourlySlicesFake(),
                batchSizeLimit,
                new Config(opts),
                startingOffset,
                new MetricRegistry()
        );
        Assertions.assertEquals(startingOffset, batchSizeLimitedResults.latest());
    }

    @Test
    public void testStubbable() {
        final LimitedResults batchSizeLimitedResults = new BatchSizeLimitedResults(
                new HourlyViewsSlices(new ArrayList<>(), 1L),
                new BatchSizeLimit(1, 1),
                new Config(opts),
                1,
                new MetricRegistry()
        );
        final LimitedResults stubResults = new StubLimitedResults();
        Assertions.assertFalse(batchSizeLimitedResults.isStub());
        Assertions.assertTrue(stubResults.isStub());
    }
}
