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
package com.teragrep.pth_06.config;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public final class HBaseConfig {

    public final String tableName;
    public final String hostname;
    public final String regionServerHostname;
    public final String zookeeperQuorum;
    public final String zookeeperClientPort;
    public final long scanCachingSize;

    public final boolean isStub;

    public HBaseConfig(final Map<String, String> opts) {
        tableName = opts.getOrDefault("hbase.tablename", "logfile");
        hostname = opts.getOrDefault("hbase.master.hostname", "localhost");
        regionServerHostname = opts.getOrDefault("hbase.regionserver.hostname", "localhost");
        zookeeperQuorum = opts.getOrDefault("hbase.zookeeper.quorum", "localhost");
        zookeeperClientPort = opts.getOrDefault("hbase.zookeeper.property.clientPort", "2181");
        scanCachingSize = Long.parseLong(opts.getOrDefault("hbase.scanCacheSize", "100"));
        isStub = false;
    }

    public Configuration asHadoopConfig() {
        Configuration config = new Configuration(false);
        config.set("hbase.master.hostname", hostname);
        config.set("hbase.regionserver.hostname", regionServerHostname);
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        return config;
    }

    public HBaseConfig() {
        tableName = "";
        hostname = "";
        regionServerHostname = "";
        zookeeperQuorum = "";
        zookeeperClientPort = "";
        scanCachingSize = 0L;
        isStub = true;
    }
}
