package com.example.redis.async.api;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.executor.CommandDescriptor;
import com.example.redis.async.executor.RedisDataStructure;
import com.example.redis.async.serialize.RedisSerializationFacade;

final class DefaultAsyncSetOperations<K, V> extends AbstractAsyncOperationsSupport implements AsyncSetOperations<K, V> {

    DefaultAsyncSetOperations(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor,
            RedisSerializationFacade serialization
    ) {
        super(
                Objects.requireNonNull(connectionProvider, "connectionProvider must not be null"),
                Objects.requireNonNull(commandExecutor, "commandExecutor must not be null"),
                Objects.requireNonNull(serialization, "serialization must not be null")
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> add(K key, V... values) {
        return guard(() -> {
            if (values.length == 0) {
                return CompletableFuture.completedFuture(0L);
            }
            return executeShared(
                    new CommandDescriptor("SADD", RedisDataStructure.SET, 1),
                    () -> connectionProvider.commands().sadd(serialization.serializeKey(key), serialization.serializeValues(java.util.List.of(values))),
                    added -> added == null ? 0L : added
            );
        });
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> remove(K key, Object... values) {
        return guard(() -> {
            if (values.length == 0) {
                return CompletableFuture.completedFuture(0L);
            }
            return executeShared(
                    new CommandDescriptor("SREM", RedisDataStructure.SET, 1),
                    () -> connectionProvider.commands().srem(serialization.serializeKey(key), serialization.serializeValues(java.util.List.of(values))),
                    removed -> removed == null ? 0L : removed
            );
        });
    }

    @Override
    public java.util.concurrent.CompletionStage<V> pop(K key) {
        return executeShared(
                new CommandDescriptor("SPOP", RedisDataStructure.SET, 1),
                () -> connectionProvider.commands().spop(serialization.serializeKey(key)),
                raw -> serialization.<V>deserializeValue(raw)
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Set<V>> pop(K key, long count) {
        return executeShared(
                new CommandDescriptor("SPOP", RedisDataStructure.SET, 1),
                () -> connectionProvider.commands().spop(serialization.serializeKey(key), count),
                raw -> raw == null ? Set.of() : serialization.<V>deserializeValueSet(raw)
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Boolean> move(K key, V value, K destinationKey) {
        return executeShared(
                new CommandDescriptor("SMOVE", RedisDataStructure.SET, 2),
                () -> connectionProvider.commands().smove(
                        serialization.serializeKey(key),
                        serialization.serializeKey(destinationKey),
                        serialization.serializeValue(value)
                ),
                Boolean.TRUE::equals
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> size(K key) {
        return executeShared(
                new CommandDescriptor("SCARD", RedisDataStructure.SET, 1),
                () -> connectionProvider.commands().scard(serialization.serializeKey(key)),
                size -> size == null ? 0L : size
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Boolean> isMember(K key, Object value) {
        return executeShared(
                new CommandDescriptor("SISMEMBER", RedisDataStructure.SET, 1),
                () -> connectionProvider.commands().sismember(serialization.serializeKey(key), serialization.serializeValue(value)),
                Boolean.TRUE::equals
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Set<V>> members(K key) {
        return executeShared(
                new CommandDescriptor("SMEMBERS", RedisDataStructure.SET, 1),
                () -> connectionProvider.commands().smembers(serialization.serializeKey(key)),
                raw -> raw == null ? Set.of() : serialization.<V>deserializeValueSet(raw)
        );
    }
}
