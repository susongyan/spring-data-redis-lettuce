package com.example.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.executor.CommandDescriptor;
import com.example.redis.async.executor.RedisDataStructure;
import com.example.redis.async.serialize.RedisSerializationFacade;
import com.example.redis.async.serialize.RedisTemplateSerializationContext;
import com.example.redis.async.support.StageAdapters;
import org.springframework.data.redis.core.RedisTemplate;

public final class AsyncRedisTemplate<K, V> implements AsyncRedisOperations<K, V> {

    private final AsyncRedisConnectionProvider connectionProvider;
    private final AsyncCommandExecutor commandExecutor;
    private final RedisSerializationFacade serialization;
    private final AsyncValueOperations<K, V> valueOperations;
    private final AsyncHashOperations<K, Object, Object> hashOperations;

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

    private long convertExpire(Long ttlInMillis, TimeUnit unit) {
        if (ttlInMillis == null) {
            return -2L;
        }
        if (ttlInMillis < 0) {
            return ttlInMillis;
        }
        return unit.convert(ttlInMillis, TimeUnit.MILLISECONDS);
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
