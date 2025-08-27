package com.teragrep.pth_06.metrics;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public final class TaskMetric implements CustomTaskMetric {

    private final String name;
    private final long value;

    public TaskMetric(final String name, final long value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long value() {
        return value;
    }
}
