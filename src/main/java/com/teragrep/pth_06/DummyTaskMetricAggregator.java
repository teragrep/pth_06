package com.teragrep.pth_06;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class DummyTaskMetricAggregator implements CustomMetric {

    @Override
    public String name() {
        // I guess names need to match with CustomTaskMetrics and here
        // guess based on lines 1150 and 1158 in SQLAppStatusListenerSuite.scala
        return "DummyTaskMetric";
    }

    @Override
    public String description() {
        return "DummyTaskMetric yadda yadda DummyTaskMetric";
    }

    @Override
    public String aggregateTaskMetrics(final long[] longs) {
        long count = 0;
        for (long l : longs) {
            if (l != Long.MIN_VALUE) {
                throw new IllegalArgumentException("long not expected, DummyTaskMetric is supposed to return only Long.MIN_VALUES");
            }
            count++;
        }
        return "Many Long.MIN_VALUES were present, can't summarize or so but their total count was: " + count;
    }

}
