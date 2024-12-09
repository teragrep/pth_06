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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Comparing Condition equality using toString() since jooq Condition uses just toString() to check for equality.
 * inherited from QueryPart
 * 
 * @see org.jooq.QueryPart
 */
public class ValidElementTest {

    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    final Document document = Assertions.assertDoesNotThrow(() -> factory.newDocumentBuilder().newDocument());

    @Test
    public void validTest() {
        Element element = document.createElement("test");
        element.setAttribute("value", "value");
        element.setAttribute("operation", "operation");
        ValidElement valid = new ValidElement(element);
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertEquals("test", valid.tag());
            Assertions.assertEquals("value", valid.value());
            Assertions.assertEquals("operation", valid.operation());
        });
    }

    @Test
    public void missingValueTest() {
        Element noValue = document.createElement("test");
        noValue.setAttribute("operation", "operation");
        ValidElement invalid1 = new ValidElement(noValue);
        Assertions.assertThrows(RuntimeException.class, () -> Assertions.assertEquals("test", invalid1.tag()));
    }

    @Test
    public void missingOperationTest() {
        Element noValue = document.createElement("test");
        noValue.setAttribute("value", "value");
        ValidElement invalid1 = new ValidElement(noValue);
        Assertions.assertThrows(RuntimeException.class, () -> Assertions.assertEquals("test", invalid1.tag()));
    }

    @Test
    public void equalityTest() {
        Element element = document.createElement("test");
        element.setAttribute("value", "value");
        element.setAttribute("operation", "operation");
        ValidElement eq1 = new ValidElement(element);
        ValidElement eq2 = new ValidElement(element);
        Assertions.assertEquals(eq1, eq2);
    }

    @Test
    public void notEqualValueTest() {
        Element element1 = document.createElement("test");
        element1.setAttribute("value", "value");
        element1.setAttribute("operation", "operation");
        Element element2 = document.createElement("test");
        element2.setAttribute("value", "notValue");
        element2.setAttribute("operation", "operation");
        ValidElement eq1 = new ValidElement(element1);
        ValidElement eq2 = new ValidElement(element2);
        Assertions.assertNotEquals(eq1, eq2);
    }

    @Test
    public void notEqualOperationTest() {
        Element element1 = document.createElement("test");
        element1.setAttribute("value", "value");
        element1.setAttribute("operation", "operation");
        Element element2 = document.createElement("test");
        element2.setAttribute("value", "value");
        element2.setAttribute("operation", "notOperation");
        ValidElement eq1 = new ValidElement(element1);
        ValidElement eq2 = new ValidElement(element2);
        Assertions.assertNotEquals(eq1, eq2);
    }

    @Test
    public void hashCodeTest() {
        Element element = document.createElement("test");
        element.setAttribute("value", "value");
        element.setAttribute("operation", "operation");
        Element element2 = document.createElement("test");
        element2.setAttribute("value", "value");
        element2.setAttribute("operation", "notOperation");
        ValidElement eq1 = new ValidElement(element);
        ValidElement eq2 = new ValidElement(element);
        ValidElement notEq = new ValidElement(element2);
        Assertions.assertEquals(eq1.hashCode(), eq2.hashCode());
        Assertions.assertNotEquals(eq1.hashCode(), notEq.hashCode());
    }

    @Test
    public void equalsHashCodeContractTest() {
        // Element is abstract, have to give prefab values to EqualsVerifier
        Element element = document.createElement("test");
        element.setAttribute("value", "value");
        element.setAttribute("operation", "operation");
        Element element2 = document.createElement("test");
        element2.setAttribute("value", "value");
        element2.setAttribute("operation", "notOperation");

        EqualsVerifier
                .forClass(ValidElement.class)
                .withNonnullFields("element")
                .withPrefabValues(Element.class, element, element2)
                .verify();
    }
}
