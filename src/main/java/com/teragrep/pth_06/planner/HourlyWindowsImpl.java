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

import com.teragrep.pth_06.ast.analyze.ScanPlanView;
import com.teragrep.pth_06.ast.analyze.View;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class HourlyWindowsImpl implements HourlyWindows {

    private final long EPOCH_HOUR = 3600L;
    private final Logger LOGGER = LoggerFactory.getLogger(HourlyWindowsImpl.class);
    private final List<View> views;
    private long currentEpoch;

    public HourlyWindowsImpl(final List<View> views, final long startEpoch) {
        this.views = views;
        this.currentEpoch = startEpoch;
    }

    public boolean hasNext() {
        return !views.stream().allMatch(View::isFinished);
    }

    public List<Result> nextHour() {
        final List<Result> hourlyResults = new ArrayList<>();
        LOGGER.debug("next hour between <{}>-<{}>", currentEpoch, currentEpoch + EPOCH_HOUR);
        for (final View view : views) {
            if (view.isFinished()) {
                LOGGER.debug("View <{}> finished, closing", view);
                view.close();
                continue;
            }

            final View viewWithingStart;
            final long viewEpoch = view.latestEpochProcessed();
            LOGGER.debug("Latest epoch from view <{}>", viewEpoch);
            if (!view.isEndOffsetWithinRange(currentEpoch)) {
                LOGGER.info("View offset <{}> is after current epoch <{}>, closing", viewEpoch, currentEpoch);
                view.close();
                continue;
            }
            else if (viewEpoch < currentEpoch) {
                LOGGER.info("Moving view offset <{}> forward to <{}>", viewEpoch, currentEpoch);
                viewWithingStart = view.viewFromOffset(currentEpoch);
            }
            else {
                viewWithingStart = view;
            }
            final List<Result> results;
            try {
                if (!viewWithingStart.isOpen()) {
                    viewWithingStart.open();
                }
                results = viewWithingStart.nextWindow(EPOCH_HOUR);
            }
            catch (final IOException e) {
                throw new RuntimeException("Error getting results: " + e.getMessage());
            }
            hourlyResults.addAll(results);
        }

        currentEpoch = currentEpoch + EPOCH_HOUR;
        return hourlyResults;
    }

    @Override
    public void close() {
        for (final View view: views) {
            if (view.isOpen()) {
                view.close();
            }
        }
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
