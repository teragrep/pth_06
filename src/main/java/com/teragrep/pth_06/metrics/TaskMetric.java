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
package com.teragrep.pth_06.metrics;

import com.teragrep.pth_06.TeragrepScan;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public final class TaskMetric implements CustomTaskMetric {

    private final String name;
    private final long value;

    /**
     * Defines a custom metric with a name and a value.
     * The TaskMetric needs to have a matching CustomMetric with the same name
     * defined in the {@link TeragrepScan#supportedCustomMetrics()}
     * in order to work properly.
     * @param name Name of the custom metric, needs to match the name in the corresponding {@link org.apache.spark.sql.connector.metric.CustomMetric}
     * @param value Long-typed value of the metric
     */
    public TaskMetric(final String name, final long value) {
        this.name = name;
        this.value = value;
    }

    /**
     * The name of this task metric.
     * NOTE: This name needs to be the same as in the matching CustomMetric
     * @return name of the task metric
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * The individual metric value
     * @return value as long type
     */
    @Override
    public long value() {
        return value;
    }
}
