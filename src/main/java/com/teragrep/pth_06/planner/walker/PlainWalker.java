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
package com.teragrep.pth_06.planner.walker;

import org.w3c.dom.Element;

/**
 * <h1>Plain Walker</h1> Regular walker.
 *
 * @since 23/09/2021
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 */
public class PlainWalker extends XmlWalker {

    /**
     * Constructor without connection. Used during unit-tests. Enables jooq-query construction.
     */
    public PlainWalker() {
        super();
    }

    /*
            @Override
            public String traverse(TreeWalker walker, String indent) {
                Node parend = walker.getCurrentNode();
                if(indent == null){
                    indent = "";
                }
                System.out.println(indent + "- " + ((Element) parend).getTagName());
                for (Node n = walker.firstChild(); n != null; n = walker.nextSibling()) {
                    return traverse(walker, indent + '\t');
                }
                walker.setCurrentNode(parend);
                return null;
            }
    */
    private String indent = "\t";

    @Override
    public Element emitLogicalOperation(String op, Object l, Object r) throws Exception {
        Element rv = null;
        String left = (String) l;
        String right = (String) r;
        if (op == null) {
            throw new Exception("Parse error, unbalanced elements. " + l.toString());
        }
        System.out.println(indent + op);
        indent += "\t";
        System.out.println(indent + left);
        System.out.println(indent + right);
        return rv;
    }

    @Override
    public Element emitUnaryOperation(String op, Element current) throws Exception {
        Element rv = null;
        if (op == null) {
            throw new Exception("Parse error, unknown operation: " + op + " expression:" + current);
        }
        System.out.println(indent + op);
        indent += "\t";
        System.out.println(indent + emitElem(current));
        return rv;
    }

    @Override
    String emitElem(Element current) {
        //            System.out.println(indent+"PlainEmitElem:"+current.getTagName());
        return current.getTagName();
    }
}
