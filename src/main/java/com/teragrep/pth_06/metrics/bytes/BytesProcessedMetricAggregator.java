package com.teragrep.pth_06.metrics.bytes;

import org.apache.spark.sql.connector.metric.CustomMetric;

public final class BytesProcessedMetricAggregator implements CustomMetric {

    public BytesProcessedMetricAggregator() {
        // 0-arg ctor required by Spark
    }

    @Override
    public String name() {
        return "BytesProcessed";
    }

    @Override
    public String description() {
        return "BytesProcessed";
    }

    @Override
    public String aggregateTaskMetrics(final long[] taskMetrics) {
        long sum = 0;
        for (final long taskMetric : taskMetrics) {
            sum += taskMetric;
        }
        return sum + " bytes";
    }
}
