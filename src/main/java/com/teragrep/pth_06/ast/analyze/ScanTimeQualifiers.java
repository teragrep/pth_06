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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class ScanTimeQualifiers {

    private final Logger LOGGER = LoggerFactory.getLogger(ScanTimeQualifiers.class);
    private final List<XMLValueExpression> earliestList;
    private final List<XMLValueExpression> latestList;

    public ScanTimeQualifiers(final ClassifiedExpressions classifiedExpressions) {
        this(classifiedExpressions.earliestList(), classifiedExpressions.latestList());
    }

    public ScanTimeQualifiers(final List<XMLValueExpression> earliestList, final List<XMLValueExpression> latestList) {
        this.earliestList = earliestList;
        this.latestList = latestList;
    }

    public long earliest() {
        if (earliestList.isEmpty()) {
            throw new IllegalStateException("Scan group did not have required time qualifiers");
        }
        else if (earliestList.size() > 1 && LOGGER.isWarnEnabled()) {
            LOGGER.warn("Multiple time qualifiers found. earliest size: <{}>", earliestList.size());
        }
        long earliest = Long.MAX_VALUE;
        for (final XMLValueExpression expression : earliestList) {
            final long expressionCalculatedValue = new CalculatedTimeQualifierValue(expression).value();
            earliest = Math.min(earliest, expressionCalculatedValue);
        }
        return earliest;
    }

    public long latest() {
        if (latestList.isEmpty()) {
            throw new IllegalStateException("Scan group did not have required time qualifiers");
        }
        else if (latestList.size() > 1 && LOGGER.isWarnEnabled()) {
            LOGGER.warn("Multiple time qualifiers found. latest size: <{}>", latestList.size());
        }
        long latest = Long.MIN_VALUE;
        for (final XMLValueExpression expression : latestList) {
            final long expressionValue = Long.parseLong(expression.value());
            latest = Math.max(latest, expressionValue);
        }
        return latest;
    }
}
