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
package com.teragrep.pth_06.planner.bloomfilter;

import com.teragrep.blf_01.Token;
import com.teragrep.pth_06.planner.TokenizedValue;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

class TokenizedValueTest {

    @Test
    void testTokenization() {
        TokenizedValue result = new TokenizedValue("test.nest");
        Set<String> tokens = result.tokens().stream().map(Token::toString).collect(Collectors.toSet());
        Assertions.assertTrue(tokens.contains("nest"));
        Assertions.assertTrue(tokens.contains("test"));
        Assertions.assertTrue(tokens.contains("."));
        Assertions.assertTrue(tokens.contains("test.nest"));
        Assertions.assertTrue(tokens.contains(".nest"));
        Assertions.assertTrue(tokens.contains("test."));
        Assertions.assertEquals(6, tokens.size());
    }

    @Test
    void testEquality() {
        TokenizedValue value1 = new TokenizedValue("test");
        TokenizedValue value2 = new TokenizedValue("test");
        Assertions.assertEquals(value1, value2);
        Assertions.assertEquals(value2, value1);
        value1.tokens();
        Assertions.assertEquals(value2, value1);
    }

    @Test
    void testNotEquals() {
        TokenizedValue value1 = new TokenizedValue("test");
        TokenizedValue value2 = new TokenizedValue("nest");
        Assertions.assertNotEquals(value1, value2);
        Assertions.assertNotEquals(value2, value1);
        Assertions.assertNotEquals(value1, null);
    }

    @Test
    void testHashCode() {
        TokenizedValue value1 = new TokenizedValue("test");
        TokenizedValue value2 = new TokenizedValue("test");
        TokenizedValue notEq = new TokenizedValue("nest");
        Assertions.assertEquals(value1.hashCode(), value2.hashCode());
        Assertions.assertNotEquals(value1.hashCode(), notEq.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        EqualsVerifier.forClass(TokenizedValue.class).withNonnullFields("value").withNonnullFields("tokenSet").verify();
    }
}
