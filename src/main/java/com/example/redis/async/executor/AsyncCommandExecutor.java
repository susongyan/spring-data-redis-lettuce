package com.example.redis.async.executor;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;

public interface AsyncCommandExecutor {

    <R, T> CompletionStage<T> execute(
            CommandDescriptor descriptor,
            Supplier<RedisFuture<R>> invocation,
            Function<R, T> decoder
    );
}
