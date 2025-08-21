package com.teragrep.pth_06.metrics.records;

import org.apache.spark.sql.connector.metric.CustomMetric;

public final class RecordsProcessedMetricAggregator implements CustomMetric {
    @Override
    public String name() {
        return "RecordsProcessed";
    }

    @Override
    public String description() {
        return "RecordsProcessed";
    }

    @Override
    public String aggregateTaskMetrics(final long[] taskMetrics) {
        long sum = 0;
        for (final long taskMetric : taskMetrics) {
            sum += taskMetric;
        }

        return sum + " records";
    }
}
