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
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class TeragrepMockScan implements Scan {

    private final CaseInsensitiveStringMap options;
    private final StructType schema;
    private final CustomTaskMetric[] metric;

    public TeragrepMockScan(CaseInsensitiveStringMap options, StructType schema) {
        this.options = options;
        this.schema = schema;
        this.metric = new CustomTaskMetric[] {
                new SuperTaskMetric()
        };
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
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
            kafkaQueryProcessor = null;
        }

        return new ArchiveMicroStreamReader(archiveQueryProcessor, kafkaQueryProcessor, config);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        System.out.println("i gief supported custom metrics in TeragrepScan");
        // see examples at sql/core/src/test/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListenerSuite.scala
        return new CustomMetric[] {
                new SuperMetric()
        };
    }

    @Override
    public CustomTaskMetric[] reportDriverMetrics() {
        System.out.println("i gief reportDriverMetrics in TeragrepScan");
        // there needs to be an Aggregator for this too, registered in supportedCustomMetrics
        // these are driver specific metrics
        return metric;
    }
}
