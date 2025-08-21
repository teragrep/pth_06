package com.teragrep.pth_06.metrics.bytes;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public final class BytesProcessedTaskMetric implements CustomTaskMetric {
    private final long value;

    public BytesProcessedTaskMetric(final long value) {
        this.value = value;
    }

    @Override
    public String name() {
        return "BytesProcessed";
    }

    @Override
    public long value() {
        return value;
    }
}
