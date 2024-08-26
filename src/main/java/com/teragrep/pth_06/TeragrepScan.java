package com.teragrep.pth_06;

import com.teragrep.pth_06.config.Config;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

public class TeragrepScan implements Scan {
    private final StructType schema;
    private final Config config;

    TeragrepScan(StructType schema, Config config) {
        this.schema = schema;
        this.config = config;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new ArchiveMicroStreamReader(config);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        // see examples at sql/core/src/test/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListenerSuite.scala

        CustomMetric[] customMetrics = new CustomMetric[1];
        customMetrics[0] = new DummyTaskMetricAggregator();
        return customMetrics;
    }

    @Override
    public CustomTaskMetric[] reportDriverMetrics() {
        // there needs to be an Aggregator for this too, registered in supportedCustomMetrics
        // these are driver specific metrics
        return new CustomTaskMetric[]{};
    }

}
