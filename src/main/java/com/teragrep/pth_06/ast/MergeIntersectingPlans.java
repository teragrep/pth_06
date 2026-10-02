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
package com.teragrep.pth_06.ast;

import com.teragrep.pth_06.ast.analyze.ScanPlan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class MergeIntersectingPlans {

    private final List<ScanPlan> scanPlans;

    public MergeIntersectingPlans(final List<ScanPlan> scanPlans) {
        this.scanPlans = scanPlans;
    }

    public List<ScanPlan> mergedRanges() {
        final List<ScanPlan> result;
        if (!scanPlans.isEmpty()) {
            final List<ScanPlan> sorted = new ArrayList<>(scanPlans);
            sorted.sort(Comparator.comparing(ScanPlan::earliest));
            result = new ArrayList<>();
            ScanPlan current = sorted.get(0);
            // interval merging
            for (int i = 1; i < sorted.size(); i++) {
                final ScanPlan next = sorted.get(i);
                if (current.mergeable(next)) {
                    current = current.merge(next);
                }
                else {
                    result.add(current);
                }
            }
            result.add(current);
        }
        else {
            result = scanPlans;
        }
        return result;
    }
}
