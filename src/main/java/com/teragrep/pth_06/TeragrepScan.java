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
import com.teragrep.pth_06.metrics.TaskMetric;
import com.teragrep.pth_06.metrics.bytes.BytesPerSecondMetricAggregator;
import com.teragrep.pth_06.metrics.bytes.BytesProcessedMetricAggregator;
import com.teragrep.pth_06.metrics.bytes.ArchiveCompressedBytesProcessedMetricAggregator;
import com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowAvgLatencyMetricAggregator;
import com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowCountMetricAggregator;
import com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowMaxLatencyMetricAggregator;
import com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowMinLatencyMetricAggregator;
import com.teragrep.pth_06.metrics.objects.ArchiveObjectsProcessedMetricAggregator;
import com.teragrep.pth_06.metrics.offsets.ArchiveOffsetMetricAggregator;
import com.teragrep.pth_06.metrics.offsets.KafkaOffsetMetricAggregator;
import com.teragrep.pth_06.metrics.records.LatestKafkaTimestampMetricAggregator;
import com.teragrep.pth_06.metrics.records.RecordsPerSecondMetricAggregator;
import com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator;
import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class TeragrepScan implements Scan {

    private final StructType schema;
    private final ArchiveMicroStreamReader stream;

    public TeragrepScan(final StructType schema, final Config config) {
        this(schema, new ArchiveMicroStreamReader(config));
    }

    public TeragrepScan(final StructType schema, final ArchiveMicroStreamReader stream) {
        this.schema = schema;
        this.stream = stream;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "teragrep";
    }

    @Override
    public MicroBatchStream toMicroBatchStream(final String checkpointLocation) {
        return stream;
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return new CustomMetric[] {
                new ArchiveDatabaseRowCountMetricAggregator(),
                new ArchiveDatabaseRowAvgLatencyMetricAggregator(),
                new ArchiveDatabaseRowMaxLatencyMetricAggregator(),
                new ArchiveDatabaseRowMinLatencyMetricAggregator(),
                new ArchiveCompressedBytesProcessedMetricAggregator(),
                new BytesProcessedMetricAggregator(),
                new BytesPerSecondMetricAggregator(),
                new RecordsProcessedMetricAggregator(),
                new RecordsPerSecondMetricAggregator(),
                new LatestKafkaTimestampMetricAggregator(),
                new ArchiveObjectsProcessedMetricAggregator(),
                new KafkaOffsetMetricAggregator(),
                new ArchiveOffsetMetricAggregator(),
        };
    }

    @Override
    public CustomTaskMetric[] reportDriverMetrics() {
        final DatasourceOffset offset = stream.mostRecentOffset();
        final List<CustomTaskMetric> metrics = new ArrayList<>();

        if (offset.getArchiveOffset() != null) {
            metrics.add(new TaskMetric("ArchiveOffset", offset.getArchiveOffset().offset()));
        }

        if (offset.getKafkaOffset() != null) {
            final Map<TopicPartition, Long> kafkaOffsets = offset.getKafkaOffset().getOffsetMap();
            metrics.add(new TaskMetric("KafkaOffset", kafkaOffsets.size()));
        }

        metrics.addAll(Arrays.asList(stream.currentDatabaseMetrics()));

        return metrics.toArray(new CustomTaskMetric[0]);
    }
}
