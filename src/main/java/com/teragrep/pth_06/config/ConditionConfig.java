package com.teragrep.pth_06.config;

import com.teragrep.pth_06.planner.walker.conditions.ElementCondition;
import org.jooq.DSLContext;

public final class ConditionConfig {
    private final DSLContext ctx;
    private final boolean streamQuery;
    private final boolean bloomEnabled;
    private final boolean withoutFilters;

    public ConditionConfig(DSLContext ctx, boolean streamQuery) {
        this.ctx = ctx;
        this.streamQuery = streamQuery;
        this.bloomEnabled = false;
        this.withoutFilters = false;
    }

    public ConditionConfig(DSLContext ctx, boolean streamQuery, boolean bloomEnabled, boolean withoutFilters) {
        this.ctx = ctx;
        this.streamQuery = streamQuery;
        this.bloomEnabled = bloomEnabled;
        this.withoutFilters = withoutFilters;
    }

    public DSLContext context() {
        return ctx;
    }

    public boolean bloomEnabled() {
        return bloomEnabled;
    }

    public boolean streamQuery() {
        return streamQuery;
    }

    public boolean withoutFilter() {
        return withoutFilters;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;
        final ConditionConfig cast = (ConditionConfig) object;
        return this.bloomEnabled == cast.bloomEnabled &&
                this.streamQuery == cast.streamQuery &&
                this.withoutFilters == cast.withoutFilters &&
                this.ctx == cast.ctx;
    }
}