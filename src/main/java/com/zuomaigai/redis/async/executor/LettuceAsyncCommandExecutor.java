package com.zuomaigai.redis.async.executor;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import com.zuomaigai.redis.async.exception.AsyncRedisExceptionTranslator;
import com.zuomaigai.redis.async.metrics.AsyncRedisMetricsRecorder;
import com.zuomaigai.redis.async.support.AsyncRedisTemplateOptions;
import com.zuomaigai.redis.async.support.StageAdapters;
import io.lettuce.core.RedisFuture;

public final class LettuceAsyncCommandExecutor implements AsyncCommandExecutor {

    private final AsyncRedisExceptionTranslator exceptionTranslator;
    private final AsyncRedisMetricsRecorder metricsRecorder;
    private final AsyncRedisTemplateOptions options;

    public LettuceAsyncCommandExecutor(
            AsyncRedisExceptionTranslator exceptionTranslator,
            AsyncRedisMetricsRecorder metricsRecorder,
            AsyncRedisTemplateOptions options
    ) {
        this.exceptionTranslator = Objects.requireNonNull(exceptionTranslator, "exceptionTranslator must not be null");
        this.metricsRecorder = Objects.requireNonNull(metricsRecorder, "metricsRecorder must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
    }

    @Override
    public <R, T> CompletionStage<T> execute(
            CommandDescriptor descriptor,
            Supplier<RedisFuture<R>> invocation,
            Function<R, T> decoder
    ) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        Objects.requireNonNull(invocation, "invocation must not be null");
        Objects.requireNonNull(decoder, "decoder must not be null");

        AsyncRedisMetricsRecorder.Sample sample = metricsRecorder.start(descriptor);

        final RedisFuture<R> redisFuture;
        try {
            redisFuture = invocation.get();
        } catch (Throwable throwable) {
            RuntimeException translated = exceptionTranslator.translate(descriptor, throwable);
            sample.failure(translated);
            return StageAdapters.failedStage(translated);
        }

        CompletionStage<T> decodedStage = StageAdapters.map(redisFuture, decoder, options.decodeExecutor());
        CompletableFuture<T> result = new CompletableFuture<>();
        decodedStage.whenComplete((value, error) -> {
            if (error != null) {
                RuntimeException translated = exceptionTranslator.translate(descriptor, error);
                sample.failure(translated);
                result.completeExceptionally(translated);
                return;
            }
            sample.success();
            result.complete(value);
        });
        return result;
    }
}
