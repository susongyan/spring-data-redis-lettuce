package com.zuomaigai.redis.async.api;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import com.zuomaigai.redis.async.executor.AsyncCommandExecutor;
import com.zuomaigai.redis.async.executor.CommandDescriptor;
import com.zuomaigai.redis.async.executor.RedisDataStructure;
import com.zuomaigai.redis.async.serialize.RedisSerializationFacade;
import io.lettuce.core.ScoredValue;
import org.springframework.data.redis.core.ZSetOperations;

final class DefaultAsyncZSetOperations<K, V> extends AbstractAsyncOperationsSupport implements AsyncZSetOperations<K, V> {

    DefaultAsyncZSetOperations(
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
    public java.util.concurrent.CompletionStage<Boolean> add(K key, V value, double score) {
        return executeShared(
                new CommandDescriptor("ZADD", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zadd(
                        serialization.serializeKey(key),
                        score,
                        serialization.serializeValue(value)
                ),
                added -> added != null && added > 0
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> add(K key, Set<ZSetOperations.TypedTuple<V>> tuples) {
        return guard(() -> {
            if (tuples.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            List<ScoredValue<byte[]>> scoredValues = serialization.serializeTypedTuples(tuples);
            return executeShared(
                    new CommandDescriptor("ZADD", RedisDataStructure.ZSET, 1),
                    () -> connectionProvider.commands().zadd(
                            serialization.serializeKey(key),
                            scoredValues.toArray(ScoredValue[]::new)
                    ),
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
                    new CommandDescriptor("ZREM", RedisDataStructure.ZSET, 1),
                    () -> connectionProvider.commands().zrem(
                            serialization.serializeKey(key),
                            serialization.serializeValues(java.util.List.of(values))
                    ),
                    removed -> removed == null ? 0L : removed
            );
        });
    }

    @Override
    public java.util.concurrent.CompletionStage<Double> incrementScore(K key, V value, double delta) {
        return executeShared(
                new CommandDescriptor("ZINCRBY", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zincrby(
                        serialization.serializeKey(key),
                        delta,
                        serialization.serializeValue(value)
                ),
                updated -> updated == null ? 0D : updated
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Double> score(K key, Object value) {
        return executeShared(
                new CommandDescriptor("ZSCORE", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zscore(
                        serialization.serializeKey(key),
                        serialization.serializeValue(value)
                ),
                current -> current == null ? null : current
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> rank(K key, Object value) {
        return executeShared(
                new CommandDescriptor("ZRANK", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zrank(
                        serialization.serializeKey(key),
                        serialization.serializeValue(value)
                ),
                current -> current == null ? null : current
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> reverseRank(K key, Object value) {
        return executeShared(
                new CommandDescriptor("ZREVRANK", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zrevrank(
                        serialization.serializeKey(key),
                        serialization.serializeValue(value)
                ),
                current -> current == null ? null : current
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Set<V>> range(K key, long start, long end) {
        return executeShared(
                new CommandDescriptor("ZRANGE", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zrange(serialization.serializeKey(key), start, end),
                raw -> raw == null ? Set.of() : new LinkedHashSet<>(serialization.<V>deserializeValueBytes(raw))
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Set<ZSetOperations.TypedTuple<V>>> rangeWithScores(K key, long start, long end) {
        return executeShared(
                new CommandDescriptor("ZRANGE", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zrangeWithScores(serialization.serializeKey(key), start, end),
                raw -> raw == null ? Set.of() : serialization.<V>deserializeTypedTuples(raw)
        );
    }

    @Override
    public java.util.concurrent.CompletionStage<Long> size(K key) {
        return executeShared(
                new CommandDescriptor("ZCARD", RedisDataStructure.ZSET, 1),
                () -> connectionProvider.commands().zcard(serialization.serializeKey(key)),
                size -> size == null ? 0L : size
        );
    }
}
