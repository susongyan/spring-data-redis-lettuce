package com.zuomaigai.redis.async.support;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

public final class StageAdapters {

    private StageAdapters() {
    }

    public static <T> CompletionStage<T> failedStage(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    public static <T, R> CompletionStage<R> map(
            CompletionStage<T> stage,
            Function<? super T, ? extends R> mapper,
            Executor executor
    ) {
        Objects.requireNonNull(stage, "stage must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");

        if (executor == null) {
            return stage.thenApply(mapper);
        }
        return stage.thenApplyAsync(mapper, executor);
    }
}
