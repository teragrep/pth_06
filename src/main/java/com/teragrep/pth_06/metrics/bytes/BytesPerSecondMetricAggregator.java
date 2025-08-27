package com.teragrep.pth_06.metrics.bytes;

import org.apache.spark.sql.connector.metric.CustomMetric;

public final class BytesPerSecondMetricAggregator implements CustomMetric {
    @Override
    public String name() {
        return "BytesPerSecond";
    }

    @Override
    public String description() {
        return "BytesPerSecond";
    }

    @Override
    public String aggregateTaskMetrics(final long[] taskMetrics) {
        if (taskMetrics.length > 0) {
            long sum = 0;
            for (final long taskMetric : taskMetrics) {
                sum += taskMetric;
            }
            return (sum/taskMetrics.length) + " b/s";
        }
        return "0 b/s";
    }
}
