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

package com.teragrep.pth06.task.kafka;

import com.teragrep.rlo_06.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * <h1>RFC5452 Reader</h1>
 *
 * Class for reading data stream from com.teragrep.rlo_06
 *
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class RFC5424Reader {

    private final ParserResultset currentResultSet;
    private final RFC5424Parser parser;

    public RFC5424Reader() {
        // subscriptions
        // Define fields we are interested
        // Main level
        RFC5424ParserSubscription subscription = new RFC5424ParserSubscription();
        subscription.add(ParserEnum.TIMESTAMP);
        subscription.add(ParserEnum.HOSTNAME);
        subscription.add(ParserEnum.MSG);

        // Structured
        RFC5424ParserSDSubscription sdSubscription = new RFC5424ParserSDSubscription();
        sdSubscription.subscribeElement("event_node_source@48577","source");
        sdSubscription.subscribeElement("event_node_relay@48577","source");
        sdSubscription.subscribeElement("event_node_source@48577","source_module");
        sdSubscription.subscribeElement("event_node_relay@48577","source_module");
        sdSubscription.subscribeElement("event_node_source@48577","hostname");
        sdSubscription.subscribeElement("event_node_relay@48577","hostname");

        sdSubscription.subscribeElement("teragrep@48577","streamname");
        sdSubscription.subscribeElement("teragrep@48577","directory");
        sdSubscription.subscribeElement("teragrep@48577","unixtime");

        // Origin
        sdSubscription.subscribeElement("origin@48577","hostname");

        // workaround cfe-06/issue/68 by subscribing the broken field
        sdSubscription.subscribeElement("rfc3164@48577","syslogtag");

        // Set up result
        currentResultSet = new ParserResultset(subscription, sdSubscription);

        parser = new RFC5424Parser(null);
    }

    public void setInputStream(InputStream inputStream) {
        parser.setInputStream(inputStream);
    }

    public boolean next() throws IOException {
        currentResultSet.clear();
        return parser.next(currentResultSet);
    }

    public ParserResultset get() {
        return currentResultSet;
    }
}
