package com.zuomaigai.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import com.zuomaigai.redis.async.connection.AsyncRedisConnectionSession;
import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import com.zuomaigai.redis.async.exception.AsyncRedisUnsupportedOperationException;
import com.zuomaigai.redis.async.executor.AsyncCommandExecutor;
import com.zuomaigai.redis.async.executor.CommandDescriptor;
import com.zuomaigai.redis.async.executor.RedisDataStructure;
import com.zuomaigai.redis.async.serialize.RedisSerializationFacade;
import com.zuomaigai.redis.async.serialize.RedisTemplateSerializationContext;
import com.zuomaigai.redis.async.support.StageAdapters;
import org.springframework.data.redis.core.RedisTemplate;

public final class AsyncRedisTemplate<K, V> implements AsyncRedisOperations<K, V> {

    private final AsyncRedisConnectionProvider connectionProvider;
    private final AsyncCommandExecutor commandExecutor;
    private final RedisSerializationFacade serialization;
    private final AsyncValueOperations<K, V> valueOperations;
    private final AsyncHashOperations<K, Object, Object> hashOperations;
    private final AsyncListOperations<K, V> listOperations;
    private final AsyncSetOperations<K, V> setOperations;
    private final AsyncZSetOperations<K, V> zSetOperations;
    private final AsyncStreamOperations<K, Object, Object> streamOperations;
    private final AsyncPubSubOperations<K, V> pubSubOperations;

    public AsyncRedisTemplate(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor,
            RedisTemplateSerializationContext serializationContext
    ) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor must not be null");
        this.serialization = new RedisSerializationFacade(
                Objects.requireNonNull(serializationContext, "serializationContext must not be null"));
        this.valueOperations = new DefaultAsyncValueOperations<>(connectionProvider, commandExecutor, serialization);
        this.hashOperations = new DefaultAsyncHashOperations<>(connectionProvider, commandExecutor, serialization);
        this.listOperations = new DefaultAsyncListOperations<>(connectionProvider, commandExecutor, serialization);
        this.setOperations = new DefaultAsyncSetOperations<>(connectionProvider, commandExecutor, serialization);
        this.zSetOperations = new DefaultAsyncZSetOperations<>(connectionProvider, commandExecutor, serialization);
        this.streamOperations = new DefaultAsyncStreamOperations<>(connectionProvider, commandExecutor, serialization);
        this.pubSubOperations = new DefaultAsyncPubSubOperations<>(connectionProvider, commandExecutor, serialization);
    }

    public static <K, V> AsyncRedisTemplate<K, V> from(
            RedisTemplate<K, V> redisTemplate,
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor
    ) {
        return new AsyncRedisTemplate<>(connectionProvider, commandExecutor, RedisTemplateSerializationContext.from(redisTemplate));
    }

    @Override
    public AsyncValueOperations<K, V> opsForValue() {
        return valueOperations;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <HK, HV> AsyncHashOperations<K, HK, HV> opsForHash() {
        return (AsyncHashOperations<K, HK, HV>) hashOperations;
    }

    @Override
    public AsyncListOperations<K, V> opsForList() {
        return listOperations;
    }

    @Override
    public AsyncSetOperations<K, V> opsForSet() {
        return setOperations;
    }

    @Override
    public AsyncZSetOperations<K, V> opsForZSet() {
        return zSetOperations;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <HK, HV> AsyncStreamOperations<K, HK, HV> opsForStream() {
        return (AsyncStreamOperations<K, HK, HV>) streamOperations;
    }

    @Override
    public AsyncPubSubOperations<K, V> opsForPubSub() {
        return pubSubOperations;
    }

    @Override
    public CompletionStage<Boolean> delete(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("DEL", RedisDataStructure.KEY, 1),
                    () -> connectionProvider.commands().del(rawKey),
                    deleted -> deleted != null && deleted.longValue() > 0
            );
        });
    }

    @Override
    public CompletionStage<Long> delete(Collection<K> keys) {
        return guard(() -> {
            if (keys.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            byte[][] rawKeys = serialization.serializeKeys(keys);
            return commandExecutor.execute(
                    new CommandDescriptor("DEL", RedisDataStructure.KEY, rawKeys.length),
                    () -> connectionProvider.commands().del(rawKeys),
                    deleted -> deleted == null ? 0L : deleted.longValue()
            );
        });
    }

    @Override
    public CompletionStage<Boolean> hasKey(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("EXISTS", RedisDataStructure.KEY, 1),
                    () -> connectionProvider.commands().exists(rawKey),
                    count -> count != null && count.longValue() > 0
            );
        });
    }

    @Override
    public CompletionStage<Boolean> expire(K key, Duration timeout) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("PEXPIRE", RedisDataStructure.KEY, 1),
                    () -> connectionProvider.commands().pexpire(rawKey, timeout.toMillis()),
                    Boolean.TRUE::equals
            );
        });
    }

    @Override
    public CompletionStage<Boolean> persist(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("PERSIST", RedisDataStructure.KEY, 1),
                    () -> connectionProvider.commands().persist(rawKey),
                    Boolean.TRUE::equals
            );
        });
    }

    @Override
    public CompletionStage<Long> getExpire(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("TTL", RedisDataStructure.KEY, 1),
                    () -> connectionProvider.commands().ttl(rawKey),
                    ttl -> ttl == null ? -2L : ttl.longValue()
            );
        });
    }

    @Override
    public CompletionStage<Long> getExpire(K key, TimeUnit unit) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            if (unit == TimeUnit.SECONDS) {
                return getExpire(key);
            }
            return commandExecutor.execute(
                    new CommandDescriptor("PTTL", RedisDataStructure.KEY, 1),
                    () -> connectionProvider.commands().pttl(rawKey),
                    ttl -> convertExpire(ttl, unit)
            );
        });
    }

    @Override
    public CompletionStage<AsyncTransactionOperations.AsyncTransactionResult> executeTransaction(
            Consumer<AsyncTransactionOperations<K, V>> callback
    ) {
        return executeTransaction(List.of(), callback);
    }

    @Override
    public CompletionStage<AsyncTransactionOperations.AsyncTransactionResult> executeTransaction(
            Collection<K> watchKeys,
            Consumer<AsyncTransactionOperations<K, V>> callback
    ) {
        return executeTransactionWithResult(watchKeys, operations -> {
            callback.accept(operations);
            return null;
        }).thenApply(AsyncTransactionOperations.AsyncTransactionExecution::result);
    }

    @Override
    public <T> CompletionStage<AsyncTransactionOperations.AsyncTransactionExecution<T>> executeTransactionWithResult(
            Function<AsyncTransactionOperations<K, V>, T> callback
    ) {
        return executeTransactionWithResult(List.of(), callback);
    }

    @Override
    public <T> CompletionStage<AsyncTransactionOperations.AsyncTransactionExecution<T>> executeTransactionWithResult(
            Collection<K> watchKeys,
            Function<AsyncTransactionOperations<K, V>, T> callback
    ) {
        return guard(() -> {
            Collection<K> actualWatchKeys = watchKeys == null ? List.of() : watchKeys;
            AsyncRedisConnectionSession session = connectionProvider.openSession();
            if (!session.supportsTransactions()) {
                try {
                    session.close();
                } catch (Throwable ignored) {
                    // no-op
                }
                throw new AsyncRedisUnsupportedOperationException(
                        "Transactions are only supported for standalone Lettuce connections");
            }

            CompletionStage<Void> watchStage = actualWatchKeys.isEmpty()
                    ? CompletableFuture.completedFuture(null)
                    : commandExecutor.execute(
                    new CommandDescriptor("WATCH", RedisDataStructure.TRANSACTION, actualWatchKeys.size()),
                    () -> session.transactionalCommands().watch(serialization.serializeKeys(actualWatchKeys)),
                    ignored -> null
            );

            CompletionStage<AsyncTransactionOperations.AsyncTransactionExecution<T>> stage = watchStage
                    .thenCompose(ignored -> commandExecutor.execute(
                            new CommandDescriptor("MULTI", RedisDataStructure.TRANSACTION, 0),
                            () -> session.transactionalCommands().multi(),
                            value -> null
                    ))
                    .thenCompose(ignored -> {
                        DefaultAsyncTransactionOperations<K, V> operations =
                                new DefaultAsyncTransactionOperations<>(session, serialization);
                        final T context;
                        try {
                            context = callback.apply(operations);
                        } catch (Throwable throwable) {
                            return discardAndFail(session, throwable);
                        }
                        return commandExecutor.execute(
                                new CommandDescriptor("EXEC", RedisDataStructure.TRANSACTION, 0),
                                () -> session.transactionalCommands().exec(),
                                operations::decode
                        ).thenApply(result -> new AsyncTransactionOperations.AsyncTransactionExecution<>(context, result));
                    });

            return closeSessionOnCompletion(session, stage);
        });
    }

    private long convertExpire(Long ttlInMillis, TimeUnit unit) {
        if (ttlInMillis == null) {
            return -2L;
        }
        if (ttlInMillis < 0) {
            return ttlInMillis;
        }
        return unit.convert(ttlInMillis, TimeUnit.MILLISECONDS);
    }

    private <T> CompletionStage<AsyncTransactionOperations.AsyncTransactionExecution<T>> discardAndFail(
            AsyncRedisConnectionSession session,
            Throwable throwable
    ) {
        CompletionStage<Void> discardStage = commandExecutor.execute(
                new CommandDescriptor("DISCARD", RedisDataStructure.TRANSACTION, 0),
                () -> session.transactionalCommands().discard(),
                value -> null
        );
        CompletableFuture<AsyncTransactionOperations.AsyncTransactionExecution<T>> result = new CompletableFuture<>();
        discardStage.whenComplete((ignored, discardError) -> {
            if (discardError != null) {
                throwable.addSuppressed(discardError);
            }
            result.completeExceptionally(throwable);
        });
        return result;
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

    private <T> CompletionStage<T> guard(ThrowingStageSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Throwable throwable) {
            return StageAdapters.failedStage(throwable);
        }
    }

    @FunctionalInterface
    private interface ThrowingStageSupplier<T> {
        CompletionStage<T> get();
    }
}
