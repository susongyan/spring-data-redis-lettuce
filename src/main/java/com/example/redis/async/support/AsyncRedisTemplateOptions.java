package com.example.redis.async.support;

import java.util.concurrent.Executor;

public final class AsyncRedisTemplateOptions {

    private final Executor decodeExecutor;

    private AsyncRedisTemplateOptions(Builder builder) {
        this.decodeExecutor = builder.decodeExecutor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Executor decodeExecutor() {
        return decodeExecutor;
    }

    public static final class Builder {

        private Executor decodeExecutor;

        private Builder() {
        }

        public Builder decodeExecutor(Executor decodeExecutor) {
            this.decodeExecutor = decodeExecutor;
            return this;
        }

        public AsyncRedisTemplateOptions build() {
            return new AsyncRedisTemplateOptions(this);
        }
    }
}
