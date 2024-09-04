package com.teragrep.pth_06.config;

import org.jooq.DSLContext;

public final class ConditionConfig {
    private final DSLContext ctx;
    private final boolean streamQuery;
    private final boolean bloomEnabled;

    public ConditionConfig(DSLContext ctx, boolean streamQuery) {
        this(ctx, streamQuery, false);
    }

    public ConditionConfig(DSLContext ctx, boolean streamQuery, boolean bloomEnabled) {
        this.ctx = ctx;
        this.streamQuery = streamQuery;
        this.bloomEnabled = bloomEnabled;
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

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;
        final ConditionConfig cast = (ConditionConfig) object;
        return this.bloomEnabled == cast.bloomEnabled &&
                this.streamQuery == cast.streamQuery &&
                this.ctx == cast.ctx;
    }
}