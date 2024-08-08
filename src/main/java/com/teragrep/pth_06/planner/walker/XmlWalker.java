/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022, 2023, 2024  Suomen Kanuuna Oy
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

/**
 * <h1>XML Walker</h1>
 * <p>
 * Abstract class to represent XmlWalker.
 *
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 * @since 23/09/2021
 */
public abstract class XmlWalker<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(XmlWalker.class);

    /**
     * Constructor without connection. Used during unit-tests. Enables jooq-query construction.
     */
    public XmlWalker() {
    }

    public T fromString(String inXml) {
        LOGGER.info("XmlWalker.fromString incoming: <{}>", inXml);
        T rv;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        Document document;
        try {
            DocumentBuilder loader = factory.newDocumentBuilder();
            document = loader.parse(new InputSource(new StringReader(inXml)));
            DocumentTraversal traversal = (DocumentTraversal) document;
            TreeWalker walker = traversal.createTreeWalker(document.getDocumentElement(),
                    NodeFilter.SHOW_ELEMENT, null, true);
            rv = traverse(walker, null);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing Document from string");
        }
        return rv;
    }

    /**
     * Walk through tree using depth-first order and generate spark-query using appropriate emit-methods.
     *
     * @param op operation String
     * @return Class which expr-part contains actual catalyst query tree
     */
    public T traverse(TreeWalker walker, String op) {
        Node node = walker.getCurrentNode();
        Element current;
        current = ((Element) node);
        T rv = null;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(" traverse incoming: <{}> with op: <{}>", current.getTagName(), op);
        }
        if ("AND".equalsIgnoreCase(current.getTagName()) || "OR".equalsIgnoreCase(current.getTagName()) || "NOT".equalsIgnoreCase(current.getTagName())) {
            op = current.getTagName();
        }
        try {
            if (node.hasChildNodes()) {
                NodeList children = node.getChildNodes();
                int count = children.getLength();
                if (count < 1 || count > 2) {
                    throw new Exception("Error, wrong number of children: <" + count + "> operation: <" + op + ">");
                }
                // get left and right
                Node left = walker.firstChild();
                T lft;
                if (count == 1) {
                    walker.setCurrentNode(left);
                    lft = traverse(walker, op);
                    rv = lft;
                }
                if (count == 2) {
                    lft = traverse(walker, op);
                    T rht = null;
                    Node right = walker.nextSibling();
                    walker.setCurrentNode(left);
                    if (right != null) {
                        walker.setCurrentNode(right);
                        rht = traverse(walker, op);
                    }
                    if (lft != null && rht != null) {
                        if (op == null) {
                            throw new Exception("Parse error, unbalanced elements <" + lft + ">");
                        }
                        rv = emitLogicalOperation(op, lft, rht);
                    } else if (lft == null) {
                        rv = rht;
                    } else {
                        rv = lft;
                    }
                }
            } else {
                // leaf
                if (op != null && op.equals("NOT")) {
                    LOGGER.debug("Emit Unary operation: <{}> l: <{}>", op, current);
                    rv = emitUnaryOperation(op, current);
                } else {
                    LOGGER.debug("EmitElem");
                    rv = emitElem(current);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error traversing <{}>, operation: <{}>", e.getMessage(), op);
        }
        walker.setCurrentNode(node);
        return rv;
    }

    /**
     * Abstract method which is called during traverse. Emits appropriate element
     *
     * @param current DOM-element
     * @return correct query according to implementation class
     */
    abstract T emitElem(Element current);

    /**
     * Abstract method which is called during traverse. Emits appropriate logical operation
     *
     * @return correct query according to implementation class
     */
    abstract T emitLogicalOperation(String op, Object left, Object right) throws Exception;

    /**
     * Abstract method which is called during traverse. Emits appropriate unary operation
     *
     * @param current DOM-element
     * @return correct query according to implementation class
     */
    abstract T emitUnaryOperation(String op, Element current) throws Exception;
}
