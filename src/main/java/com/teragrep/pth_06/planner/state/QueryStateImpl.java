package com.teragrep.pth_06.planner.state;

import com.codahale.metrics.MetricRegistry;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.BatchSizeLimitedResults;
import com.teragrep.pth_06.planner.HourlySlices;
import com.teragrep.pth_06.planner.LimitedResults;


public final class QueryStateImpl implements QueryState {
    private final Config config;
    private final HourlySlices slices;
    private final MetricRegistry metricsRegistry;
    private final LimitedResults results;

    public QueryStateImpl(Config config, HourlySlices slices, long previousOffset, MetricRegistry metricsRegistry) {
        this(config, slices, new BatchSizeLimitedResults(slices, config, previousOffset, metricsRegistry), metricsRegistry);
    }

    public QueryStateImpl(Config config, HourlySlices slices, LimitedResults results, MetricRegistry metricsRegistry) {
        this.config = config;
        this.slices = slices;
        this.metricsRegistry = metricsRegistry;
        this.results = results;
    }

    @Override
    public LimitedResults results() {
        if (results.isStub()) {
            throw new UnsupportedOperationException("results was stub");
        }
        return results;
    }

    @Override
    public QueryState next() {
        final QueryState nextState;
        if (slices.isStub() || !slices.hasNext()) {
            nextState = new StubQueryState();
        } else {
            final long nextOffset = results.latest();
            nextState = new QueryStateImpl(config, slices, nextOffset, metricsRegistry);
        }
        return nextState;
    }

    @Override
    public boolean hasNext() {
        return results.isStub();
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
