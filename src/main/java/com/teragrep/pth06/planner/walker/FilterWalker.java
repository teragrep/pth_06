/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

package com.teragrep.pth06.planner.walker;

import com.teragrep.pth06.planner.walker.filter.*;
import org.w3c.dom.Element;

/**
 * <h2>Filter Walker</h2>
 *
 * Walker for filters.
 */
public class FilterWalker extends XmlWalker {
    @Override
    FilterOp emitElem(Element current) {
        String tag = current.getTagName();
        String value = current.getAttribute("value");
        String operation = current.getAttribute("operation");

        FilterOp filterOp;

        if (tag.equalsIgnoreCase("index")) {
            filterOp = new FilterColumnEquals("index", value.toLowerCase());

            if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                filterOp = new FilterNot(filterOp);
            }
        }
        // stream
         else if (tag.equalsIgnoreCase("sourcetype")) {
            filterOp = new FilterColumnEquals("sourcetype", value.toLowerCase());

            if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                filterOp = new FilterNot(filterOp);
            }
        }
        // host
        else if (tag.equalsIgnoreCase("host")) {
            filterOp = new FilterColumnEquals("host", value.toLowerCase());

            if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                filterOp = new FilterNot(filterOp);
            }
        }
        else {
            throw new IllegalStateException("unknown operation at " + current);
        }
        return filterOp;
    }

    public FilterOp fromString(String inXml) throws Exception {
        return super.fromString(inXml);
    }

    @Override
    public FilterOp emitLogicalOperation(String op, Object l, Object r) throws Exception{
        FilterOp rv;
        FilterOp left = (FilterOp)l;
        FilterOp right = (FilterOp)r;

        if(op == null){
            throw new Exception("Parse error, unbalanced elements. "+left.toString());
        }
        if(op.equalsIgnoreCase("AND")) {
            rv = new FilterAnd(left,right);
        } else if(op.equalsIgnoreCase("OR")){
            rv = new FilterOr(left,right);
        }
        else {
            throw new IllegalStateException("Parse error, unknown logical operation. op:"+op+ " expression:"+left);
        }
        return rv;
    }

    @Override
    FilterOp emitUnaryOperation(String op, Element current) throws Exception {
        FilterOp filterOp = emitElem(current);

        if(op == null){
            throw new Exception("Parse error, Unknown unary operation:"+op+" expression:"+filterOp.toString());
        }

        if(filterOp != null) {
            if(op.equalsIgnoreCase("NOT")){
                filterOp = new FilterNot(filterOp);
            }
            else {
                throw new Exception("Parse error, unsupported unary operation. op:"+op+ " expression:"+filterOp.toString());
            }
        }
        return filterOp;
    }
}
