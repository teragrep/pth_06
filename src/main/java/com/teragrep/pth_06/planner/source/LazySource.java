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
package com.teragrep.pth_06.planner.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public final class LazySource implements HBaseSource {

    private final Logger LOGGER = LoggerFactory.getLogger(LazySource.class);

    private final HBaseSource origin;
    private final AtomicReference<HBaseSource> reference;

    public LazySource(final Configuration configuration) {
        this(new SourceFromConfig(configuration));
    }

    public LazySource(final HBaseSource origin) {
        this(origin, new AtomicReference<>(new DisconnectedSource()));
    }

    private LazySource(final HBaseSource origin, final AtomicReference<HBaseSource> reference) {
        this.origin = origin;
        this.reference = reference;
    }

    @Override
    public Connection connection() {
        return reference.updateAndGet(source -> {
            if (!source.isOpen()) {
                LOGGER.info("Opened new HBase connection");
                return new CachedSource(origin.connection());
            }
            return source;
        }).connection();
    }

    @Override
    public boolean isOpen() {
        return reference.get().isOpen();
    }

    @Override
    public void close() {
        reference.getAndUpdate(source -> {
            if (source.isOpen()) {
                LOGGER.info("Closing HBase source");
                source.close();
            }
            return new DisconnectedSource();
        });
    }
}
