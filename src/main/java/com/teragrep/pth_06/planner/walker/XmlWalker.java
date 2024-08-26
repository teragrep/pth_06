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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.traversal.DocumentTraversal;
import org.w3c.dom.traversal.NodeFilter;
import org.w3c.dom.traversal.TreeWalker;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <h1>XML Walker</h1> Abstract class to represent XmlWalker.
 *
 * @since 23/09/2021
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 */
public abstract class XmlWalker {

    private final Logger LOGGER = LoggerFactory.getLogger(XmlWalker.class);

    /**
     * Constructor without connection. Used during unit-tests. Enables jooq-query construction.
     */
    public XmlWalker() {

    }

    public <T> T fromString(String inXml) throws Exception {
        Object rv;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder loader = factory.newDocumentBuilder();
        Document document = loader.parse(new InputSource(new StringReader(inXml)));

        DocumentTraversal traversal = (DocumentTraversal) document;
        LOGGER.info("XmlWalker.fromString incoming:" + inXml);

        TreeWalker walker = traversal
                .createTreeWalker(document.getDocumentElement(), NodeFilter.SHOW_ELEMENT, null, true);
        rv = traverse(walker, (String) null);
        //        if (rv != null) {
        //            System.out.println("XmlWalker.fromString() value:" + ((T) rv).toString());
        //        }
        return (T) rv;
    }

    /**
     * Walk through tree using depth-first order and generate spark-query using appropriate emit-methods.
     * 
     * @param walker
     * @param op     operation String
     * @return Class which expr-part contains actual catalyst query tree
     * @throws Exception
     */
    public <T> T traverse(TreeWalker walker, String op) throws Exception {
        Node parend = walker.getCurrentNode();
        Element current = ((Element) parend);
        Object rv = null;
        LOGGER.debug(" traverse incoming:" + current.getTagName());

        try {
            if (
                current.getTagName().equalsIgnoreCase("AND") || current.getTagName().equalsIgnoreCase("OR")
                        || current.getTagName().equalsIgnoreCase("NOT")
            ) {
                op = current.getTagName();
            }

            if (parend.hasChildNodes()) {
                NodeList children = parend.getChildNodes();
                int count = children.getLength();
                if (count < 1 || count > 2) {
                    throw new Exception("Error, wrong number of children:" + count + " op:" + op);
                }
                // get left and right
                Node left = walker.firstChild();
                Object lft;
                switch (count) {
                    case 1: {
                        LOGGER.debug("  1 child incoming:" + current + " left=" + left + " op:" + op);

                        //rv = emitLogicalOperation(op, lft, null);
                        walker.setCurrentNode(left);
                        lft = traverse(walker, op);
                        LOGGER.debug("--Traverse 1 child op:" + op + " lft=" + lft);
                        rv = lft;
                        break;
                    }
                    case 2: {
                        lft = traverse(walker, op);
                        Object rht = null;
                        Node right = walker.nextSibling();
                        walker.setCurrentNode(left);
                        //                        System.out.println("traverse right:"+right);
                        if (right != null) {
                            walker.setCurrentNode(right);
                            rht = traverse(walker, op);
                        }
                        if (lft != null && rht != null) {
                            if (op == null) {
                                throw new Exception("Parse error, unbalanced elements. " + lft.toString());
                            }
                            rv = emitLogicalOperation(op, lft, rht);
                        }
                        else if (lft == null) {
                            rv = rht;
                        }
                        else if (rht == null) {
                            rv = lft;
                        }
                        break;
                    }
                }
            }
            else {
                // leaf
                if (op != null && op.equals("NOT")) {
                    LOGGER.debug("Emit Unary operation op:" + op + " l:" + current);
                    rv = emitUnaryOperation(op, current);
                }
                else {
                    LOGGER.debug("EmitElem");
                    rv = emitElem(current);
                }
            }
            walker.setCurrentNode(parend);
            //if(rv != null)
            //    System.out.println("XmlWalker.traverse type:"+rv.getClass().getName() +" returns:"+((T)rv).toString());
            return (T) rv;
        }
        catch (Exception e) {
            LOGGER.error(e.toString());
        }
        return null;
    }

    /**
     * Abstract method which is called during traverse. Emits appropriate element
     *
     * @param         <T> returned class
     * @param current DOM-element
     * @return correct query according to implementation class
     */
    abstract <T> T emitElem(Element current);

    /**
     * Abstract method which is called during traverse. Emits appropriate logical operation
     * 
     * @param <T> returned class
     * @return correct query according to implementation class
     */
    abstract <T> T emitLogicalOperation(String op, Object left, Object right) throws Exception;

    /**
     * Abstract method which is called during traverse. Emits appropriate unary operation
     * 
     * @param         <T> returned class
     * @param current DOM-element
     * @return correct query according to implementation class
     */
    abstract <T> T emitUnaryOperation(String op, Element current) throws Exception;
    // escape special chars inside value

    /**
     * Add escapes to special characters and replace '*' with '%'
     *
     * @param input string
     * @return escaped string
     */
    public static String escapeSpecialCharacters(String input) {
        final List<String> specialCharacters = Arrays.asList("\\", "*", "+", "?", "%");
        if (input.equals("*")) {
            return "%";
        }
        else {
            return Arrays.stream(input.split("")).map((c) -> {
                if (specialCharacters.contains(c))
                    return "\\" + c;
                else
                    return c;
            }).collect(Collectors.joining());
        }
    }
}
