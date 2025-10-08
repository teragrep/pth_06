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
package com.teragrep.pth_06;

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MockTeragrepDatasource implements DataSourceRegister, TableProvider, Table, SupportsRead {

    private final String name = "teragrep";

    private final Logger LOGGER = LoggerFactory.getLogger(MockTeragrepDatasource.class);
    private final StructType schema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("origin", DataTypes.StringType, false, new MetadataBuilder().build()),
    });

    @Override
    public String name() {
        return name;
    }

    @Override
    public String shortName() {
        return name;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return schema;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return this;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return () -> {
            Config config = new Config(options);

            ArchiveQuery archiveQueryProcessor = new MockArchiveQueryProcessor(
                    "<index operation=\"EQUALS\" value=\"f17_v2\"/>"
            );

            KafkaQuery kafkaQueryProcessor;
            if (config.isKafkaEnabled) {
                Consumer<byte[], byte[]> kafkaConsumer = MockKafkaConsumerFactory.getConsumer();

                kafkaQueryProcessor = new KafkaQueryProcessor(kafkaConsumer);
            }
            else {
                kafkaQueryProcessor = new StubKafkaQuery();
            }

            ArchiveMicroStreamReader stream = new ArchiveMicroStreamReader(
                    config,
                    archiveQueryProcessor,
                    kafkaQueryProcessor,
                    new StubHBaseQuery()
            );
            return new TeragrepScan(schema, stream);
        };
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Stream.of(TableCapability.MICRO_BATCH_READ).collect(Collectors.toSet());
    }
}
