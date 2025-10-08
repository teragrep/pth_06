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
package com.teragrep.pth_06.ast.xml;

import com.teragrep.pth_06.ast.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public final class XMLQuery {

    private final Logger LOGGER = LoggerFactory.getLogger(XMLQuery.class);
    private final String xmlString;

    public XMLQuery(final String xmlString) {
        this.xmlString = xmlString;
    }

    public Expression asAST() {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
        }
        catch (final ParserConfigurationException e) {
            throw new RuntimeException("Error creating document builder: " + e.getMessage());
        }
        final Document document;
        try {
            document = builder.parse(new InputSource(new StringReader(xmlString)));
        }
        catch (final IOException | SAXException e) {
            throw new RuntimeException("Error parsing XML to elements: " + e.getMessage());
        }
        final Element root = document.getDocumentElement();
        return elementToNode(root);
    }

    private Expression elementToNode(final Element element) {
        final String tagName = element.getTagName();
        LOGGER.info("Incoming element <{}>", tagName);
        final Expression result;
        switch (tagName.toLowerCase()) {
            case "and":
                final List<Expression> andExpMembers = visitLogical(element);
                final Expression andLeft = andExpMembers.get(0);
                final Expression andRight = andExpMembers.get(1);
                result = new AndExpression(andLeft, andRight);
                break;
            case "or":
                final List<Expression> expressions = visitLogical(element);
                final Expression left = expressions.get(0);
                final Expression right = expressions.get(1);
                result = new OrExpression(left, right);
                break;
            case "index":
                result = visitLeaf(element, Expression.Tag.INDEX);
                break;
            case "host":
                result = visitLeaf(element, Expression.Tag.HOST);
                break;
            case "sourcetype":
                result = visitLeaf(element, Expression.Tag.SOURCETYPE);
                break;
            case "earliest":
            case "index_earliest":
                result = visitLeaf(element, Expression.Tag.EARLIEST);
                break;
            case "latest":
            case "index_latest":
                result = visitLeaf(element, Expression.Tag.LATEST);
                break;
            case "indexstatement":
                result = visitLeaf(element, Expression.Tag.INDEXSTATEMENT);
                break;
            default:
                throw new IllegalArgumentException("Unsupported element <" + tagName + ">");
        }
        return result;
    }

    private Expression visitLeaf(final Element element, final Expression.Tag tag) {
        final String value = element.getAttribute("value");
        final String operation = element.getAttribute("operation");
        return new XMLValueExpressionImpl(value, operation, tag);
    }

    private List<Expression> visitLogical(final Element element) {
        final List<Expression> expressions = new ArrayList<>();
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            final Node node = childNodes.item(i);
            if (node instanceof Element) {
                final Element nodeElement = (Element) node;
                // recursive call
                final Expression exp = elementToNode(nodeElement);
                expressions.add(exp);
            }
            else {
                throw new IllegalArgumentException("Element children contained a non Element node");
            }
        }
        return expressions;
    }
}
