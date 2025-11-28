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

import java.util.Map;

/**
 * <h1>Config</h1> Holds all the settings for the program. Including MariaDB, Kafka and Spark.
 *
 * @since 17/08/2021
 * @author Mikko Kortelainen
 * @author Eemeli Hukka
 */
public final class Config {

    public final String query;
    public final ArchiveConfig archiveConfig;
    public final KafkaConfig kafkaConfig;

    public final BatchConfig batchConfig;
    public final AuditConfig auditConfig;
    public final HBaseConfig hBaseConfig;

    public final LoggingConfig loggingConfig;

    public final boolean isArchiveEnabled;
    public final boolean isHbaseEnabled;
    public final boolean isKafkaEnabled;

    public final boolean isMetadataQuery;

    public Config(Map<String, String> opts) {
        this.query = opts.get("queryXML");
        if (this.query == null) {
            throw new IllegalArgumentException("no queryXML provided");
        }

        batchConfig = new BatchConfig(opts);
        auditConfig = new AuditConfig(opts);

        isArchiveEnabled = opts.getOrDefault("archive.enabled", "false").equalsIgnoreCase("true");
        isHbaseEnabled = opts.getOrDefault("hbase.enabled", "false").equalsIgnoreCase("true");
        if (isArchiveEnabled) {
            archiveConfig = new ArchiveConfig(opts);
        }
        else {
            archiveConfig = new ArchiveConfig();
        }

        isKafkaEnabled = opts.getOrDefault("kafka.enabled", "false").equalsIgnoreCase("true");
        if (isKafkaEnabled) {
            kafkaConfig = new KafkaConfig(opts);
        }
        else {
            kafkaConfig = new KafkaConfig();
        }

        if (isHbaseEnabled) {
            hBaseConfig = new HBaseConfig(opts);
        }
        else {
            hBaseConfig = new HBaseConfig();
        }

        // check that at least one datasource is enabled
        if (!isArchiveEnabled && !isKafkaEnabled) {
            throw new IllegalStateException("No datasources enabled");
        }

        // fetch metadata (defaults to false)
        isMetadataQuery = opts.getOrDefault("metadataQuery.enabled", "false").equalsIgnoreCase("true");

        loggingConfig = new LoggingConfigImpl(opts);
    }
}
