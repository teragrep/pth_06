package com.teragrep.pth_06.config;

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
}
