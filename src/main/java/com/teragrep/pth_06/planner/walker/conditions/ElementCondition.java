/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2024  Suomen Kanuuna Oy
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

package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.blf_01.Tokenizer;
import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Optional;

/**
 * Creates a query condition from provided dom element
 */
public final class ElementCondition {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementCondition.class);

    private final Element element;
    private final ConditionConfig config;

    private void validate(Element element) {
        if (element.getTagName() == null) {
            throw new IllegalStateException("Tag name for Element was null");
        }
        if (!element.hasAttribute("operation")) {
            throw new IllegalStateException("Could not find specified or default value for 'operation' attribute from Element");
        }
        if (!element.hasAttribute("value")) {
            throw new IllegalStateException("Could not find specified or default value for 'value' attribute from Element");
        }
    }

    public ElementCondition(Element element, ConditionConfig config) {
        this.element = element;
        this.config = config;
    }

    public Condition condition() {
        validate(element);
        final String tag = element.getTagName();
        final String operation = element.getAttribute("operation");
        Optional<Condition> optional = Optional.empty();
        switch (tag.toLowerCase()) {
            case "index":
                QueryCondition index = new IndexCondition(element, config.streamQuery());
                optional = Optional.of(index.condition());
                break;
            case "sourcetype":
                QueryCondition sourceType = new SourceTypeCondition(element, config.streamQuery());
                optional = Optional.of(sourceType.condition());
                break;
            case "host":
                QueryCondition host = new HostCondition(element, config.streamQuery());
                optional = Optional.of(host.condition());
                break;
        }
        if (!config.streamQuery()) {
            // Handle also time qualifiers
            if ("earliest".equalsIgnoreCase(tag) || "index_earliest".equalsIgnoreCase(tag)) {
                QueryCondition earliest = new EarliestCondition(element);
                optional = Optional.of(earliest.condition());
            }
            if ("latest".equalsIgnoreCase(tag) || "index_latest".equalsIgnoreCase(tag)) {
                QueryCondition latest = new LatestCondition(element);
                optional = Optional.of(latest.condition());
            }
            // value search
            if ("indexstatement".equalsIgnoreCase(tag) && "EQUALS".equals(operation) && config.bloomEnabled()) {
                IndexStatementCondition indexStatementCondition =
                        new IndexStatementCondition(element, config, new Tokenizer(32));
                optional = Optional.of(indexStatementCondition.condition());
            }
        }
        if (!optional.isPresent()) {
            throw new IllegalStateException("Unsupported Element tag " + tag);
        }
        Condition rv = optional.get();
        LOGGER.debug("Query condition: <{}>", rv);
        return rv;
    }
}
