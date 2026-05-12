package com.zuomaigai.redis.async.metrics;

import com.zuomaigai.redis.async.executor.CommandDescriptor;

public final class NoopAsyncRedisMetricsRecorder implements AsyncRedisMetricsRecorder {

    public static final NoopAsyncRedisMetricsRecorder INSTANCE = new NoopAsyncRedisMetricsRecorder();

    private static final Sample NOOP_SAMPLE = new Sample() {
        @Override
        public void success() {
        }

        @Override
        public void failure(Throwable error) {
        }
    };

    private NoopAsyncRedisMetricsRecorder() {
    }

    @Override
    public Sample start(CommandDescriptor descriptor) {
        return NOOP_SAMPLE;
    }
}
