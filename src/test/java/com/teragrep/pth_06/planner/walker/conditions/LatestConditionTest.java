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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.Condition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 * 
 * @see org.jooq.QueryPart
 */
public class LatestConditionTest {

    @Test
    public void conditionTest() {
        String e = "(\n" + "  \"journaldb\".\"logfile\".\"logdate\" <= date '1970-01-01'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 1000)\n"
                + ")";
        Condition elementCondition = new LatestCondition("1000").condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    public void conditionUpdatedTest() {
        String e = "(\n" + "  \"journaldb\".\"logfile\".\"logdate\" <= date '2000-01-01'\n"
                + "  and (UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H')) <= 946720800)\n"
                + ")";
        Condition elementCondition = new LatestCondition("946720800").condition();
        Assertions.assertEquals(e, elementCondition.toString());
    }

    @Test
    public void equalsTest() {
        LatestCondition eq1 = new LatestCondition("946720800");
        eq1.condition();
        LatestCondition eq2 = new LatestCondition("946720800");
        LatestCondition eq3 = new LatestCondition("946720800");
        eq3.condition();
        LatestCondition eq4 = new LatestCondition("946720800");
        Assertions.assertEquals(eq1, eq2);
        Assertions.assertEquals(eq3, eq4);
    }

    @Test
    public void notEqualsTest() {
        LatestCondition eq1 = new LatestCondition("946720800");
        LatestCondition notEq = new LatestCondition("1000");
        Assertions.assertNotEquals(eq1, notEq);
    }

    @Test
    public void hashCodeTest() {
        LatestCondition eq1 = new LatestCondition("946720800");
        LatestCondition eq2 = new LatestCondition("946720800");
        LatestCondition notEq = new LatestCondition("1000");
        Assertions.assertEquals(eq1.hashCode(), eq2.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), notEq.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier.forClass(LatestCondition.class).withNonnullFields("value").verify();
    }
}
