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
package com.teragrep.pth_06.planner.walker.conditions;

import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

/** true if input value regex like comparedTo value, compared against BLOOMDB.FILTERTYPE.PATTERN by default */
public final class RegexLikeCondition implements QueryCondition {

    private final Field<String> valueField;
    private final Field<String> comparedToField;

    public RegexLikeCondition(String input) {
        this(DSL.val(input), BLOOMDB.FILTERTYPE.PATTERN);
    }

    public RegexLikeCondition(Field<String> input) {
        this(input, BLOOMDB.FILTERTYPE.PATTERN);
    }

    public RegexLikeCondition(String input, String comparedTo) {
        this(DSL.val(input), DSL.val(comparedTo));
    }

    public RegexLikeCondition(String input, Field<String> comparedTo) {
        this(DSL.val(input), comparedTo);
    }

    public RegexLikeCondition(Field<String> input, String comparedTo) {
        this(input, DSL.val(comparedTo));
    }

    public RegexLikeCondition(Field<String> valueField, Field<String> comparedToField) {
        this.valueField = valueField;
        this.comparedToField = comparedToField;
    }

    public Condition condition() {
        return valueField.likeRegex(comparedToField);
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || object.getClass() != this.getClass())
            return false;
        final RegexLikeCondition cast = (RegexLikeCondition) object;
        return valueField.equals(cast.valueField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueField);
    }
}
