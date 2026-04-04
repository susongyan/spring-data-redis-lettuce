package com.example.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.executor.CommandDescriptor;
import com.example.redis.async.executor.RedisDataStructure;
import com.example.redis.async.serialize.RedisSerializationFacade;
import com.example.redis.async.support.StageAdapters;
import io.lettuce.core.KeyValue;
import io.lettuce.core.SetArgs;

final class DefaultAsyncValueOperations<K, V> implements AsyncValueOperations<K, V> {

    private final AsyncRedisConnectionProvider connectionProvider;
    private final AsyncCommandExecutor commandExecutor;
    private final RedisSerializationFacade serialization;

    DefaultAsyncValueOperations(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor,
            RedisSerializationFacade serialization
    ) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor must not be null");
        this.serialization = Objects.requireNonNull(serialization, "serialization must not be null");
    }

    @Override
    public CompletionStage<V> get(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("GET", RedisDataStructure.VALUE, 1),
                    () -> connectionProvider.commands().get(rawKey),
                    rawValue -> serialization.<V>deserializeValue(rawValue)
            );
        });
    }

    @Override
    public CompletionStage<Void> set(K key, V value) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawValue = serialization.serializeValue(value);
            return commandExecutor.execute(
                    new CommandDescriptor("SET", RedisDataStructure.VALUE, 1),
                    () -> connectionProvider.commands().set(rawKey, rawValue),
                    ignored -> null
            );
        });
    }

    @Override
    public CompletionStage<Void> set(K key, V value, Duration ttl) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawValue = serialization.serializeValue(value);
            SetArgs args = SetArgs.Builder.px(ttl.toMillis());
            return commandExecutor.execute(
                    new CommandDescriptor("SET", RedisDataStructure.VALUE, 1),
                    () -> connectionProvider.commands().set(rawKey, rawValue, args),
                    ignored -> null
            );
        });
    }

    @Override
    public CompletionStage<Boolean> setIfAbsent(K key, V value) {
        return guard(() -> setWithArgs("SETNX", key, value, SetArgs.Builder.nx()));
    }

    @Override
    public CompletionStage<Boolean> setIfPresent(K key, V value) {
        return guard(() -> setWithArgs("SETXX", key, value, SetArgs.Builder.xx()));
    }

    @Override
    public CompletionStage<V> getAndSet(K key, V value) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawValue = serialization.serializeValue(value);
            return commandExecutor.execute(
                    new CommandDescriptor("GETSET", RedisDataStructure.VALUE, 1),
                    () -> connectionProvider.commands().getset(rawKey, rawValue),
                    previousValue -> serialization.<V>deserializeValue(previousValue)
            );
        });
    }

    @Override
    public CompletionStage<List<V>> multiGet(Collection<K> keys) {
        return guard(() -> {
            if (keys.isEmpty()) {
                return CompletableFuture.completedFuture(List.of());
            }
            byte[][] rawKeys = serialization.serializeKeys(keys);
            return commandExecutor.execute(
                    new CommandDescriptor("MGET", RedisDataStructure.VALUE, rawKeys.length),
                    () -> connectionProvider.commands().mget(rawKeys),
                    values -> serialization.<V>deserializeValueList((List<KeyValue<byte[], byte[]>>) values)
            );
        });
    }

    @Override
    public CompletionStage<Void> multiSet(Map<K, V> map) {
        return guard(() -> {
            if (map.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            Map<byte[], byte[]> serialized = serialization.serializeValueMap(map);
            return commandExecutor.execute(
                    new CommandDescriptor("MSET", RedisDataStructure.VALUE, serialized.size()),
                    () -> connectionProvider.commands().mset(serialized),
                    ignored -> null
            );
        });
    }

    @Override
    public CompletionStage<Long> increment(K key) {
        return increment(key, 1L);
    }

    @Override
    public CompletionStage<Long> increment(K key, long delta) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("INCRBY", RedisDataStructure.VALUE, 1),
                    () -> connectionProvider.commands().incrby(rawKey, delta),
                    value -> value == null ? 0L : value.longValue()
            );
        });
    }

    @Override
    public CompletionStage<Long> decrement(K key) {
        return decrement(key, 1L);
    }

    @Override
    public CompletionStage<Long> decrement(K key, long delta) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("DECRBY", RedisDataStructure.VALUE, 1),
                    () -> connectionProvider.commands().decrby(rawKey, delta),
                    value -> value == null ? 0L : value.longValue()
            );
        });
    }

    private CompletionStage<Boolean> setWithArgs(String command, K key, V value, SetArgs args) {
        byte[] rawKey = serialization.serializeKey(key);
        byte[] rawValue = serialization.serializeValue(value);
        return commandExecutor.execute(
                new CommandDescriptor(command, RedisDataStructure.VALUE, 1),
                () -> connectionProvider.commands().set(rawKey, rawValue, args),
                result -> result != null && "OK".equalsIgnoreCase(result)
        );
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
