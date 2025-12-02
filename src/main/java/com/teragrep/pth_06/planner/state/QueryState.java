package com.teragrep.pth_06.planner.state;

import com.teragrep.pth_06.Stubbable;
import com.teragrep.pth_06.planner.LimitedResults;

public interface QueryState extends Stubbable {

    public abstract LimitedResults results();

    public abstract QueryState next();

    public abstract boolean hasNext();
}
