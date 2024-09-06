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

import org.w3c.dom.Element;

public class ValidElement {

    private final Element element;

    private void validate() {
        if (element.getTagName() == null) {
            throw new IllegalStateException("Tag name for Element was null");
        }
        if (!element.hasAttribute("operation")) {
            throw new IllegalStateException(
                    "Could not find specified or default value for 'operation' attribute from Element"
            );
        }
        if (!element.hasAttribute("value")) {
            throw new IllegalStateException(
                    "Could not find specified or default value for 'value' attribute from Element"
            );
        }
    }

    public ValidElement(Element element) {
        this.element = element;
    }

    public String tag() {
        validate();
        return element.getTagName();
    }

    public String value() {
        validate();
        return element.getAttribute("value");
    }

    public String operation() {
        validate();
        return element.getAttribute("operation");
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final ValidElement cast = (ValidElement) object;
        boolean equalName = this.element.getTagName().equals(cast.element.getTagName());
        boolean equalOperation = this.element.getAttribute("operation").equals(cast.element.getAttribute("operation"));
        boolean equalValue = this.element.getAttribute("value").equals(cast.element.getAttribute("value"));
        return equalName && equalOperation && equalValue;
    }
}
