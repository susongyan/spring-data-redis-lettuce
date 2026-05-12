package com.zuomaigai.redis.async.metrics;

import com.zuomaigai.redis.async.executor.CommandDescriptor;

public interface AsyncRedisMetricsRecorder {

    Sample start(CommandDescriptor descriptor);

    interface Sample {
        void success();

        void failure(Throwable error);
    }
}
