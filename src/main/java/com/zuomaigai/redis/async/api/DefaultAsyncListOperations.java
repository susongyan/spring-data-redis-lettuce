package com.zuomaigai.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import com.zuomaigai.redis.async.executor.AsyncCommandExecutor;
import com.zuomaigai.redis.async.executor.CommandDescriptor;
import com.zuomaigai.redis.async.executor.RedisDataStructure;
import com.zuomaigai.redis.async.serialize.RedisSerializationFacade;
import io.lettuce.core.KeyValue;

final class DefaultAsyncListOperations<K, V> extends AbstractAsyncOperationsSupport implements AsyncListOperations<K, V> {

    DefaultAsyncListOperations(
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
    public java.util.concurrent.CompletionStage<List<V>> range(K key, long start, long end) {
        return executeShared(
                new CommandDescriptor("LRANGE", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().lrange(serialization.serializeKey(key), start, end),
                rawValues -> serialization.<V>deserializeValueBytes(rawValues)
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Void> trim(K key, long start, long end) {
        return executeShared(
                new CommandDescriptor("LTRIM", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().ltrim(serialization.serializeKey(key), start, end),
                ignored -> null
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> size(K key) {
        return executeShared(
                new CommandDescriptor("LLEN", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().llen(serialization.serializeKey(key)),
                value -> value == null ? 0L : value
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> leftPush(K key, V value) {
        return executeShared(
                new CommandDescriptor("LPUSH", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().lpush(serialization.serializeKey(key), serialization.serializeValue(value)),
                length -> length == null ? 0L : length
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> leftPushAll(K key, Collection<V> values) {
        return guard(() -> {
            if (values.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            return executeShared(
                    new CommandDescriptor("LPUSH", RedisDataStructure.LIST, 1),
                    () -> connectionProvider.commands().lpush(serialization.serializeKey(key), serialization.serializeValues(values)),
                    length -> length == null ? 0L : length
            );
        });
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> rightPush(K key, V value) {
        return executeShared(
                new CommandDescriptor("RPUSH", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().rpush(serialization.serializeKey(key), serialization.serializeValue(value)),
                length -> length == null ? 0L : length
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> rightPushAll(K key, Collection<V> values) {
        return guard(() -> {
            if (values.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            return executeShared(
                    new CommandDescriptor("RPUSH", RedisDataStructure.LIST, 1),
                    () -> connectionProvider.commands().rpush(serialization.serializeKey(key), serialization.serializeValues(values)),
                    length -> length == null ? 0L : length
            );
        });
    }

    @Override
    public java.util.concurrent.CompletionStage<V> leftPop(K key) {
        return executeShared(
                new CommandDescriptor("LPOP", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().lpop(serialization.serializeKey(key)),
                rawValue -> serialization.<V>deserializeValue(rawValue)
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<V> rightPop(K key) {
        return executeShared(
                new CommandDescriptor("RPOP", RedisDataStructure.LIST, 1),
                () -> connectionProvider.commands().rpop(serialization.serializeKey(key)),
                rawValue -> serialization.<V>deserializeValue(rawValue)
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<BlockingPopResult<K, V>> leftPop(Duration timeout, Collection<K> keys) {
        return blockingPop("BLPOP", timeout, keys, true);
    }

    @Override
    public java.util.concurrent.CompletionStage<BlockingPopResult<K, V>> rightPop(Duration timeout, Collection<K> keys) {
        return blockingPop("BRPOP", timeout, keys, false);
    }

    private java.util.concurrent.CompletionStage<BlockingPopResult<K, V>> blockingPop(
            String command,
            Duration timeout,
            Collection<K> keys,
            boolean left
    ) {
        return guard(() -> {
            if (keys.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            double timeoutSeconds = timeout.toMillis() / 1000D;
            byte[][] rawKeys = serialization.serializeKeys(keys);
            return executeDedicated(
                    new CommandDescriptor(command, RedisDataStructure.LIST, rawKeys.length),
                    session -> left ? session.commands().blpop(timeoutSeconds, rawKeys)
                            : session.commands().brpop(timeoutSeconds, rawKeys),
                    this::decodeBlockingPop
            );
        });
    }

    private BlockingPopResult<K, V> decodeBlockingPop(KeyValue<byte[], byte[]> raw) {
        if (raw == null || !raw.hasValue()) {
            return null;
        }
        return new BlockingPopResult<>(
                serialization.deserializeKey(raw.getKey()),
                serialization.deserializeValue(raw.getValue())
        );
    }
}
