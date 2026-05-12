package com.zuomaigai.redis.async.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import com.zuomaigai.redis.async.executor.AsyncCommandExecutor;
import com.zuomaigai.redis.async.executor.CommandDescriptor;
import com.zuomaigai.redis.async.executor.RedisDataStructure;
import com.zuomaigai.redis.async.serialize.RedisSerializationFacade;
import com.zuomaigai.redis.async.support.StageAdapters;
import io.lettuce.core.KeyValue;

final class DefaultAsyncHashOperations<K, HK, HV> implements AsyncHashOperations<K, HK, HV> {

    private final AsyncRedisConnectionProvider connectionProvider;
    private final AsyncCommandExecutor commandExecutor;
    private final RedisSerializationFacade serialization;

    DefaultAsyncHashOperations(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor,
            RedisSerializationFacade serialization
    ) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor must not be null");
        this.serialization = Objects.requireNonNull(serialization, "serialization must not be null");
    }

    @Override
    public CompletionStage<HV> get(K key, HK hashKey) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawHashKey = serialization.serializeHashKey(hashKey);
            return commandExecutor.execute(
                    new CommandDescriptor("HGET", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hget(rawKey, rawHashKey),
                    rawValue -> serialization.<HV>deserializeHashValue(rawValue)
            );
        });
    }

    @Override
    public CompletionStage<Boolean> hasKey(K key, HK hashKey) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawHashKey = serialization.serializeHashKey(hashKey);
            return commandExecutor.execute(
                    new CommandDescriptor("HEXISTS", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hexists(rawKey, rawHashKey),
                    Boolean.TRUE::equals
            );
        });
    }

    @Override
    public CompletionStage<Void> put(K key, HK hashKey, HV value) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawHashKey = serialization.serializeHashKey(hashKey);
            byte[] rawValue = serialization.serializeHashValue(value);
            return commandExecutor.execute(
                    new CommandDescriptor("HSET", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hset(rawKey, rawHashKey, rawValue),
                    ignored -> null
            );
        });
    }

    @Override
    public CompletionStage<Boolean> putIfAbsent(K key, HK hashKey, HV value) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawHashKey = serialization.serializeHashKey(hashKey);
            byte[] rawValue = serialization.serializeHashValue(value);
            return commandExecutor.execute(
                    new CommandDescriptor("HSETNX", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hsetnx(rawKey, rawHashKey, rawValue),
                    Boolean.TRUE::equals
            );
        });
    }

    @Override
    public CompletionStage<Void> putAll(K key, Map<HK, HV> map) {
        return guard(() -> {
            if (map.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            byte[] rawKey = serialization.serializeKey(key);
            Map<byte[], byte[]> rawValues = serialization.serializeHashMap(map);
            return commandExecutor.execute(
                    new CommandDescriptor("HMSET", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hmset(rawKey, rawValues),
                    ignored -> null
            );
        });
    }

    @Override
    public CompletionStage<List<HV>> multiGet(K key, Collection<HK> hashKeys) {
        return guard(() -> {
            if (hashKeys.isEmpty()) {
                return CompletableFuture.completedFuture(List.of());
            }
            byte[] rawKey = serialization.serializeKey(key);
            byte[][] rawHashKeys = serialization.serializeHashKeys(hashKeys);
            return commandExecutor.execute(
                    new CommandDescriptor("HMGET", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hmget(rawKey, rawHashKeys),
                    values -> serialization.<HV>deserializeHashValueList((List<KeyValue<byte[], byte[]>>) values)
            );
        });
    }

    @Override
    public CompletionStage<Long> delete(K key, Object... hashKeys) {
        return guard(() -> {
            if (hashKeys.length == 0) {
                return CompletableFuture.completedFuture(0L);
            }
            byte[] rawKey = serialization.serializeKey(key);
            byte[][] rawHashKeys = serialization.serializeHashKeys(List.of(hashKeys));
            return commandExecutor.execute(
                    new CommandDescriptor("HDEL", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hdel(rawKey, rawHashKeys),
                    deleted -> deleted == null ? 0L : deleted.longValue()
            );
        });
    }

    @Override
    public CompletionStage<Map<HK, HV>> entries(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("HGETALL", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hgetall(rawKey),
                    entries -> serialization.<HK, HV>deserializeHashEntries((Map<byte[], byte[]>) entries)
            );
        });
    }

    @Override
    public CompletionStage<Set<HK>> keys(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("HKEYS", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hkeys(rawKey),
                    keys -> serialization.<HK>deserializeHashKeySet((List<byte[]>) keys)
            );
        });
    }

    @Override
    public CompletionStage<List<HV>> values(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("HVALS", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hvals(rawKey),
                    values -> serialization.<HV>deserializeHashValues((List<byte[]>) values)
            );
        });
    }

    @Override
    public CompletionStage<Long> size(K key) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            return commandExecutor.execute(
                    new CommandDescriptor("HLEN", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hlen(rawKey),
                    value -> value == null ? 0L : value.longValue()
            );
        });
    }

    @Override
    public CompletionStage<Long> increment(K key, HK hashKey, long delta) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawHashKey = serialization.serializeHashKey(hashKey);
            return commandExecutor.execute(
                    new CommandDescriptor("HINCRBY", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hincrby(rawKey, rawHashKey, delta),
                    value -> value == null ? 0L : value.longValue()
            );
        });
    }

    @Override
    public CompletionStage<Double> increment(K key, HK hashKey, double delta) {
        return guard(() -> {
            byte[] rawKey = serialization.serializeKey(key);
            byte[] rawHashKey = serialization.serializeHashKey(hashKey);
            return commandExecutor.execute(
                    new CommandDescriptor("HINCRBYFLOAT", RedisDataStructure.HASH, 1),
                    () -> connectionProvider.commands().hincrbyfloat(rawKey, rawHashKey, delta),
                    value -> value == null ? 0D : value.doubleValue()
            );
        });
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
