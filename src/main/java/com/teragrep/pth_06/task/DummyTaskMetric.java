package com.teragrep.pth_06.task;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class DummyTaskMetric implements CustomTaskMetric {

    @Override
    public String name() {
        return "DummyTaskMetric";
    }

    @Override
    public long value() {
        return Long.MIN_VALUE;
    }

}
