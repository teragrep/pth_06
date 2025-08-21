package com.teragrep.pth_06.metrics.records;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public final class RecordsProcessedTaskMetric implements CustomTaskMetric {
    private final long value;

    public RecordsProcessedTaskMetric(final long value) {
        this.value = value;
    }

    @Override
    public String name() {
        return "RecordsProcessed";
    }

    @Override
    public long value() {
        return value;
    }
}
