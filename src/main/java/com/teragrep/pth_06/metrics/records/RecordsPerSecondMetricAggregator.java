package com.teragrep.pth_06.metrics.records;

import org.apache.spark.sql.connector.metric.CustomMetric;

public final class RecordsPerSecondMetricAggregator implements CustomMetric {
    @Override
    public String name() {
        return "RecordsPerSecond";
    }

    @Override
    public String description() {
        return "RecordsPerSecond";
    }

    @Override
    public String aggregateTaskMetrics(final long[] taskMetrics) {
        if (taskMetrics.length > 0) {
            long sum = 0;
            for (final long taskMetric : taskMetrics) {
                sum += taskMetric;
            }
            return (sum/taskMetrics.length) + " records/s";
        }
        return "0 records/s";
    }
}
