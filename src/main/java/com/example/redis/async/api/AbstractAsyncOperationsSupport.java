package com.example.redis.async.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.connection.AsyncRedisConnectionSession;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.executor.CommandDescriptor;
import com.example.redis.async.serialize.RedisSerializationFacade;
import com.example.redis.async.support.StageAdapters;
import io.lettuce.core.RedisFuture;

abstract class AbstractAsyncOperationsSupport {

    protected final AsyncRedisConnectionProvider connectionProvider;
    protected final AsyncCommandExecutor commandExecutor;
    protected final RedisSerializationFacade serialization;

    protected AbstractAsyncOperationsSupport(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor,
            RedisSerializationFacade serialization
    ) {
        this.connectionProvider = connectionProvider;
        this.commandExecutor = commandExecutor;
        this.serialization = serialization;
    }

    protected <R, T> CompletionStage<T> executeShared(
            CommandDescriptor descriptor,
            Supplier<RedisFuture<R>> invocation,
            Function<R, T> decoder
    ) {
        return guard(() -> commandExecutor.execute(descriptor, invocation, decoder));
    }

    protected <R, T> CompletionStage<T> executeDedicated(
            CommandDescriptor descriptor,
            Function<AsyncRedisConnectionSession, RedisFuture<R>> invocation,
            Function<R, T> decoder
    ) {
        return guard(() -> {
            AsyncRedisConnectionSession session = connectionProvider.openSession();
            try {
                CompletionStage<T> stage = commandExecutor.execute(
                        descriptor,
                        () -> invocation.apply(session),
                        decoder
                );
                return closeSessionOnCompletion(session, stage);
            } catch (Throwable throwable) {
                closeQuietly(session, throwable);
                throw throwable;
            }
        });
    }

    protected <T> CompletionStage<T> guard(ThrowingStageSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Throwable throwable) {
            return StageAdapters.failedStage(throwable);
        }
    }

    private <T> CompletionStage<T> closeSessionOnCompletion(
            AsyncRedisConnectionSession session,
            CompletionStage<T> stage
    ) {
        CompletableFuture<T> result = new CompletableFuture<>();
        stage.whenComplete((value, error) -> {
            Throwable outcome = error;
            try {
                session.close();
            } catch (Throwable closeError) {
                if (outcome == null) {
                    outcome = closeError;
                } else {
                    outcome.addSuppressed(closeError);
                }
            }
            if (outcome != null) {
                result.completeExceptionally(outcome);
                return;
            }
            result.complete(value);
        });
        return result;
    }

    private void closeQuietly(AsyncRedisConnectionSession session, Throwable originalError) {
        try {
            session.close();
        } catch (Throwable closeError) {
            originalError.addSuppressed(closeError);
        }
    }

    @FunctionalInterface
    protected interface ThrowingStageSupplier<T> {
        CompletionStage<T> get();
    }
}
