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
package com.teragrep.pth_06.task.s3;

import com.teragrep.rlo_06.Fragment;
import com.teragrep.rlo_06.RFC5424Frame;
import com.teragrep.rlo_06.SDVector;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

final class EventToSource {

    private final ByteBuffer sourceConcatenationBuffer;
    private final SDVector eventNodeSourceSource;
    private final SDVector eventNodeRelaySource;
    private final SDVector eventNodeSourceSourceModule;
    private final SDVector eventNodeRelaySourceModule;
    private final SDVector eventNodeSourceHostname;
    private final SDVector eventNodeRelayHostname;

    EventToSource() {
        this(ByteBuffer.allocateDirect(256 * 1024));
    }

    private EventToSource(ByteBuffer sourceConcatenationBuffer) {
        this(
                sourceConcatenationBuffer,
                new SDVector("event_node_source@48577", "source"),
                new SDVector("event_node_relay@48577", "source"),
                new SDVector("event_node_source@48577", "source_module"),
                new SDVector("event_node_relay@48577", "source_module"),
                new SDVector("event_node_source@48577", "hostname"),
                new SDVector("event_node_relay@48577", "hostname")
        );
    }

    private EventToSource(
            final ByteBuffer sourceConcatenationBuffer,
            final SDVector eventNodeSourceSource,
            final SDVector eventNodeRelaySource,
            final SDVector eventNodeSourceSourceModule,
            final SDVector eventNodeRelaySourceModule,
            final SDVector eventNodeSourceHostname,
            final SDVector eventNodeRelayHostname
    ) {
        this.sourceConcatenationBuffer = sourceConcatenationBuffer;
        this.eventNodeSourceSource = eventNodeSourceSource;
        this.eventNodeRelaySource = eventNodeRelaySource;
        this.eventNodeSourceSourceModule = eventNodeSourceSourceModule;
        this.eventNodeRelaySourceModule = eventNodeRelaySourceModule;
        this.eventNodeSourceHostname = eventNodeSourceHostname;
        this.eventNodeRelayHostname = eventNodeRelayHostname;
    }

    UTF8String asUTF8StringFrom(final RFC5424Frame rfc5424Frame) {
        return UTF8String.fromBytes(source(rfc5424Frame));
    }

    byte[] source(final RFC5424Frame rfc5424Frame) {
        //input is produced from SD element event_node_source@48577 by
        // concatenating "source_module:hostname:source". in case
        //if event_node_source@48577 is not available use event_node_relay@48577.
        //If neither are present, use null value.

        sourceConcatenationBuffer.clear();

        Fragment sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSourceModule);
        if (sourceModuleFragment.isStub) {
            sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySourceModule);
        }

        byte[] source_module;
        if (!sourceModuleFragment.isStub) {
            source_module = sourceModuleFragment.toBytes();
        }
        else {
            source_module = new byte[] {};
        }

        Fragment sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceHostname);
        if (sourceHostnameFragment.isStub) {
            sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeRelayHostname);
        }

        byte[] source_hostname;
        if (!sourceHostnameFragment.isStub) {
            source_hostname = sourceHostnameFragment.toBytes();
        }
        else {
            source_hostname = new byte[] {};
        }

        Fragment sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSource);
        if (sourceSourceFragment.isStub) {
            sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySource);
        }

        final byte[] source_source;
        if (!sourceSourceFragment.isStub) {
            source_source = sourceSourceFragment.toBytes();
        }
        else {
            source_source = new byte[] {};
        }

        // source_module:hostname:source"
        sourceConcatenationBuffer.put(source_module);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_hostname);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_source);

        sourceConcatenationBuffer.flip();
        final byte[] input = new byte[sourceConcatenationBuffer.remaining()];
        sourceConcatenationBuffer.get(input);

        return input;

    }
}
