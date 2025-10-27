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

import com.teragrep.pth_06.Stubbable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;

/** Logical plan for a row key range scan of HBase */
public interface ScanPlan extends Stubbable {

    /** Prepares the logical plan to a HBase Scan object */
    public abstract Scan toScan();

    /** new ScanRange with new earliest value if inside the scope, otherwise no changes */
    public abstract ScanPlan rangeFromEarliest(long earliest);

    /** new ScanRange with new latest value if inside the scope, otherwise no changes */
    public abstract ScanPlan rangeUntilLatest(long latest);

    /** Returns stub when new range is outside the original range */
    public abstract ScanPlan toRangeBetween(long earliest, long latest);

    public abstract long streamId();

    public abstract long earliest();

    public abstract long latest();

    public abstract FilterList filterList();

    /**
     * Returns true if the ranges overlap or touch
     * <p>
     * e.g. [10, 20] overlaps with [19, 20] and also with [20,30]
     * </p>
     */
    public abstract boolean mergeable(ScanPlan scanPlan);

    /** Merge two ScanRanges with the smaller earliest and larger latest values */
    public abstract ScanPlan merge(ScanPlan scanPlan);

}
