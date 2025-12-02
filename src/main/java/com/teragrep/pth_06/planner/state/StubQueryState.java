package com.teragrep.pth_06.planner.state;

import com.teragrep.pth_06.planner.LimitedResults;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public final class StubQueryState implements QueryState {

    @Override
    public LimitedResults results() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public QueryState next() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public boolean isStub() {
        return true;
    }
}
