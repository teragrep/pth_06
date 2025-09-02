package com.teragrep.pth_06.metrics.records;

import org.apache.spark.sql.connector.metric.CustomMetric;

public final class LatestKafkaTimestampMetricAggregator implements CustomMetric {

    public LatestKafkaTimestampMetricAggregator() {
        // 0-arg ctor required by Spark
    }

    @Override
    public String name() {
        return "LatestKafkaTimestamp";
    }

    @Override
    public String description() {
        return "LatestKafkaTimestamp";
    }

    @Override
    public String aggregateTaskMetrics(final long[] taskMetrics) {
        long latestTimestamp = 0;
        for (final long ts : taskMetrics) {
            if (ts > latestTimestamp) {
                latestTimestamp = ts;
            }
        }

        return String.valueOf(latestTimestamp);
    }
}
