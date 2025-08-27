package com.teragrep.pth_06.metrics.objects;

import org.apache.spark.sql.connector.metric.CustomMetric;

public final class ObjectsProcessedMetricAggregator implements CustomMetric {

    public ObjectsProcessedMetricAggregator() {
        // 0-arg ctor required by Spark
    }

    @Override
    public String name() {
        return "ObjectsProcessed";
    }

    @Override
    public String description() {
        return "ObjectsProcessed";
    }

    @Override
    public String aggregateTaskMetrics(final long[] taskMetrics) {
        long sum = 0;
        for (final long taskMetric : taskMetrics) {
            sum += taskMetric;
        }

        return sum + " objects";
    }
}
