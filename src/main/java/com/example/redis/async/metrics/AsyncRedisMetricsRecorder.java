package com.example.redis.async.metrics;

import com.example.redis.async.executor.CommandDescriptor;

public interface AsyncRedisMetricsRecorder {

    Sample start(CommandDescriptor descriptor);

    interface Sample {
        void success();

        void failure(Throwable error);
    }
}
