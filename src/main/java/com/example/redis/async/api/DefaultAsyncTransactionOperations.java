package com.example.redis.async.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import com.example.redis.async.connection.AsyncRedisConnectionSession;
import com.example.redis.async.serialize.RedisSerializationFacade;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.SetArgs;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.springframework.data.redis.core.ZSetOperations;

final class DefaultAsyncTransactionOperations<K, V> implements AsyncTransactionOperations<K, V> {

    private final RedisAsyncCommands<byte[], byte[]> commands;
    private final RedisSerializationFacade serialization;
    private final Recorder recorder = new Recorder();
    private final AsyncTransactionValueOperations<K, V> valueOperations;
    private final AsyncTransactionHashOperations<K, Object, Object> hashOperations;
    private final AsyncTransactionListOperations<K, V> listOperations;
    private final AsyncTransactionSetOperations<K, V> setOperations;
    private final AsyncTransactionZSetOperations<K, V> zSetOperations;

    DefaultAsyncTransactionOperations(
            AsyncRedisConnectionSession session,
            RedisSerializationFacade serialization
    ) {
        this.commands = Objects.requireNonNull(session, "session must not be null").transactionalCommands();
        this.serialization = Objects.requireNonNull(serialization, "serialization must not be null");
        this.valueOperations = new TransactionalValueOperations();
        this.hashOperations = new TransactionalHashOperations();
        this.listOperations = new TransactionalListOperations();
        this.setOperations = new TransactionalSetOperations();
        this.zSetOperations = new TransactionalZSetOperations();
    }

    AsyncTransactionResult decode(TransactionResult transactionResult) {
        return recorder.decode(transactionResult);
    }

    @Override
    public AsyncTransactionValueOperations<K, V> opsForValue() {
        return valueOperations;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <HK, HV> AsyncTransactionHashOperations<K, HK, HV> opsForHash() {
        return (AsyncTransactionHashOperations<K, HK, HV>) hashOperations;
    }

    @Override
    public AsyncTransactionListOperations<K, V> opsForList() {
        return listOperations;
    }

    @Override
    public AsyncTransactionSetOperations<K, V> opsForSet() {
        return setOperations;
    }

    @Override
    public AsyncTransactionZSetOperations<K, V> opsForZSet() {
        return zSetOperations;
    }

    @Override
    public AsyncTransactionCommand<Boolean> delete(K key) {
        return queue(
                () -> commands.del(serialization.serializeKey(key)),
                deleted -> deleted != null && deleted > 0
        );
    }

    @Override
    public AsyncTransactionCommand<Long> delete(Collection<K> keys) {
        byte[][] rawKeys = serialization.serializeKeys(keys);
        return queue(
                () -> commands.del(rawKeys),
                deleted -> deleted == null ? 0L : deleted
        );
    }

    @Override
    public AsyncTransactionCommand<Boolean> hasKey(K key) {
        return queue(
                () -> commands.exists(serialization.serializeKey(key)),
                count -> count != null && count > 0
        );
    }

    @Override
    public AsyncTransactionCommand<Boolean> expire(K key, Duration timeout) {
        return queue(
                () -> commands.pexpire(serialization.serializeKey(key), timeout.toMillis()),
                Boolean.TRUE::equals
        );
    }

    @Override
    public AsyncTransactionCommand<Boolean> persist(K key) {
        return queue(
                () -> commands.persist(serialization.serializeKey(key)),
                Boolean.TRUE::equals
        );
    }

    private <R, T> AsyncTransactionCommand<T> queue(
            Supplier<RedisFuture<R>> invocation,
            Function<R, T> decoder
    ) {
        return recorder.queue(invocation, decoder);
    }

    private final class TransactionalValueOperations implements AsyncTransactionValueOperations<K, V> {

        @Override
        public AsyncTransactionCommand<V> get(K key) {
            return queue(
                    () -> commands.get(serialization.serializeKey(key)),
                    rawValue -> serialization.<V>deserializeValue(rawValue)
            );
        }

        @Override
        public AsyncTransactionCommand<Void> set(K key, V value) {
            return queue(
                    () -> commands.set(serialization.serializeKey(key), serialization.serializeValue(value)),
                    ignored -> null
            );
        }

        @Override
        public AsyncTransactionCommand<Void> set(K key, V value, Duration ttl) {
            return queue(
                    () -> commands.set(
                            serialization.serializeKey(key),
                            serialization.serializeValue(value),
                            SetArgs.Builder.px(ttl.toMillis())
                    ),
                    ignored -> null
            );
        }

        @Override
        public AsyncTransactionCommand<Boolean> setIfAbsent(K key, V value) {
            return queue(
                    () -> commands.set(
                            serialization.serializeKey(key),
                            serialization.serializeValue(value),
                            SetArgs.Builder.nx()
                    ),
                    result -> result != null && "OK".equalsIgnoreCase(result)
            );
        }

        @Override
        public AsyncTransactionCommand<Boolean> setIfPresent(K key, V value) {
            return queue(
                    () -> commands.set(
                            serialization.serializeKey(key),
                            serialization.serializeValue(value),
                            SetArgs.Builder.xx()
                    ),
                    result -> result != null && "OK".equalsIgnoreCase(result)
            );
        }

        @Override
        public AsyncTransactionCommand<V> getAndSet(K key, V value) {
            return queue(
                    () -> commands.getset(serialization.serializeKey(key), serialization.serializeValue(value)),
                    previous -> serialization.<V>deserializeValue(previous)
            );
        }

        @Override
        public AsyncTransactionCommand<List<V>> multiGet(Collection<K> keys) {
            byte[][] rawKeys = serialization.serializeKeys(keys);
            return queue(
                    () -> commands.mget(rawKeys),
                    values -> serialization.<V>deserializeValueList((List<KeyValue<byte[], byte[]>>) values)
            );
        }

        @Override
        public AsyncTransactionCommand<Void> multiSet(Map<K, V> map) {
            return queue(
                    () -> commands.mset(serialization.serializeValueMap(map)),
                    ignored -> null
            );
        }

        @Override
        public AsyncTransactionCommand<Long> increment(K key) {
            return increment(key, 1L);
        }

        @Override
        public AsyncTransactionCommand<Long> increment(K key, long delta) {
            return queue(
                    () -> commands.incrby(serialization.serializeKey(key), delta),
                    value -> value == null ? 0L : value
            );
        }

        @Override
        public AsyncTransactionCommand<Long> decrement(K key) {
            return decrement(key, 1L);
        }

        @Override
        public AsyncTransactionCommand<Long> decrement(K key, long delta) {
            return queue(
                    () -> commands.decrby(serialization.serializeKey(key), delta),
                    value -> value == null ? 0L : value
            );
        }
    }

    private final class TransactionalHashOperations implements AsyncTransactionHashOperations<K, Object, Object> {

        @Override
        public AsyncTransactionCommand<Object> get(K key, Object hashKey) {
            return queue(
                    () -> commands.hget(serialization.serializeKey(key), serialization.serializeHashKey(hashKey)),
                    raw -> serialization.deserializeHashValue(raw)
            );
        }

        @Override
        public AsyncTransactionCommand<Boolean> hasKey(K key, Object hashKey) {
            return queue(
                    () -> commands.hexists(serialization.serializeKey(key), serialization.serializeHashKey(hashKey)),
                    Boolean.TRUE::equals
            );
        }

        @Override
        public AsyncTransactionCommand<Void> put(K key, Object hashKey, Object value) {
            return queue(
                    () -> commands.hset(
                            serialization.serializeKey(key),
                            serialization.serializeHashKey(hashKey),
                            serialization.serializeHashValue(value)
                    ),
                    ignored -> null
            );
        }

        @Override
        public AsyncTransactionCommand<Boolean> putIfAbsent(K key, Object hashKey, Object value) {
            return queue(
                    () -> commands.hsetnx(
                            serialization.serializeKey(key),
                            serialization.serializeHashKey(hashKey),
                            serialization.serializeHashValue(value)
                    ),
                    Boolean.TRUE::equals
            );
        }

        @Override
        public AsyncTransactionCommand<Void> putAll(K key, Map<Object, Object> map) {
            return queue(
                    () -> commands.hmset(serialization.serializeKey(key), serialization.serializeHashMap(map)),
                    ignored -> null
            );
        }

        @Override
        public AsyncTransactionCommand<List<Object>> multiGet(K key, Collection<Object> hashKeys) {
            return queue(
                    () -> commands.hmget(
                            serialization.serializeKey(key),
                            serialization.serializeHashKeys(hashKeys)
                    ),
                    values -> serialization.<Object>deserializeHashValueList((List<KeyValue<byte[], byte[]>>) values)
            );
        }

        @Override
        public AsyncTransactionCommand<Long> delete(K key, Object... hashKeys) {
            return queue(
                    () -> commands.hdel(
                            serialization.serializeKey(key),
                            serialization.serializeHashKeys(List.of(hashKeys))
                    ),
                    deleted -> deleted == null ? 0L : deleted
            );
        }

        @Override
        public AsyncTransactionCommand<Map<Object, Object>> entries(K key) {
            return queue(
                    () -> commands.hgetall(serialization.serializeKey(key)),
                    values -> serialization.<Object, Object>deserializeHashEntries((Map<byte[], byte[]>) values)
            );
        }

        @Override
        public AsyncTransactionCommand<Set<Object>> keys(K key) {
            return queue(
                    () -> commands.hkeys(serialization.serializeKey(key)),
                    values -> serialization.<Object>deserializeHashKeySet((List<byte[]>) values)
            );
        }

        @Override
        public AsyncTransactionCommand<List<Object>> values(K key) {
            return queue(
                    () -> commands.hvals(serialization.serializeKey(key)),
                    values -> serialization.<Object>deserializeHashValues((List<byte[]>) values)
            );
        }

        @Override
        public AsyncTransactionCommand<Long> size(K key) {
            return queue(
                    () -> commands.hlen(serialization.serializeKey(key)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<Long> increment(K key, Object hashKey, long delta) {
            return queue(
                    () -> commands.hincrby(
                            serialization.serializeKey(key),
                            serialization.serializeHashKey(hashKey),
                            delta
                    ),
                    updated -> updated == null ? 0L : updated
            );
        }

        @Override
        public AsyncTransactionCommand<Double> increment(K key, Object hashKey, double delta) {
            return queue(
                    () -> commands.hincrbyfloat(
                            serialization.serializeKey(key),
                            serialization.serializeHashKey(hashKey),
                            delta
                    ),
                    updated -> updated == null ? 0D : updated
            );
        }
    }

    private final class TransactionalListOperations implements AsyncTransactionListOperations<K, V> {

        @Override
        public AsyncTransactionCommand<List<V>> range(K key, long start, long end) {
            return queue(
                    () -> commands.lrange(serialization.serializeKey(key), start, end),
                    raw -> serialization.<V>deserializeValueBytes(raw)
            );
        }

        @Override
        public AsyncTransactionCommand<Void> trim(K key, long start, long end) {
            return queue(
                    () -> commands.ltrim(serialization.serializeKey(key), start, end),
                    ignored -> null
            );
        }

        @Override
        public AsyncTransactionCommand<Long> size(K key) {
            return queue(
                    () -> commands.llen(serialization.serializeKey(key)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<Long> leftPush(K key, V value) {
            return queue(
                    () -> commands.lpush(serialization.serializeKey(key), serialization.serializeValue(value)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<Long> leftPushAll(K key, Collection<V> values) {
            return queue(
                    () -> commands.lpush(serialization.serializeKey(key), serialization.serializeValues(values)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<Long> rightPush(K key, V value) {
            return queue(
                    () -> commands.rpush(serialization.serializeKey(key), serialization.serializeValue(value)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<Long> rightPushAll(K key, Collection<V> values) {
            return queue(
                    () -> commands.rpush(serialization.serializeKey(key), serialization.serializeValues(values)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<V> leftPop(K key) {
            return queue(
                    () -> commands.lpop(serialization.serializeKey(key)),
                    raw -> serialization.<V>deserializeValue(raw)
            );
        }

        @Override
        public AsyncTransactionCommand<V> rightPop(K key) {
            return queue(
                    () -> commands.rpop(serialization.serializeKey(key)),
                    raw -> serialization.<V>deserializeValue(raw)
            );
        }
    }

    private final class TransactionalSetOperations implements AsyncTransactionSetOperations<K, V> {

        @Override
        public AsyncTransactionCommand<Long> add(K key, V... values) {
            return queue(
                    () -> commands.sadd(serialization.serializeKey(key), serialization.serializeValues(List.of(values))),
                    added -> added == null ? 0L : added
            );
        }

        @Override
        public AsyncTransactionCommand<Long> remove(K key, Object... values) {
            return queue(
                    () -> commands.srem(serialization.serializeKey(key), serialization.serializeValues(List.of(values))),
                    removed -> removed == null ? 0L : removed
            );
        }

        @Override
        public AsyncTransactionCommand<V> pop(K key) {
            return queue(
                    () -> commands.spop(serialization.serializeKey(key)),
                    raw -> serialization.<V>deserializeValue(raw)
            );
        }

        @Override
        public AsyncTransactionCommand<Set<V>> pop(K key, long count) {
            return queue(
                    () -> commands.spop(serialization.serializeKey(key), count),
                    raw -> raw == null ? Set.of() : serialization.<V>deserializeValueSet(raw)
            );
        }

        @Override
        public AsyncTransactionCommand<Boolean> move(K key, V value, K destinationKey) {
            return queue(
                    () -> commands.smove(
                            serialization.serializeKey(key),
                            serialization.serializeKey(destinationKey),
                            serialization.serializeValue(value)
                    ),
                    Boolean.TRUE::equals
            );
        }

        @Override
        public AsyncTransactionCommand<Long> size(K key) {
            return queue(
                    () -> commands.scard(serialization.serializeKey(key)),
                    size -> size == null ? 0L : size
            );
        }

        @Override
        public AsyncTransactionCommand<Boolean> isMember(K key, Object value) {
            return queue(
                    () -> commands.sismember(serialization.serializeKey(key), serialization.serializeValue(value)),
                    Boolean.TRUE::equals
            );
        }

        @Override
        public AsyncTransactionCommand<Set<V>> members(K key) {
            return queue(
                    () -> commands.smembers(serialization.serializeKey(key)),
                    raw -> raw == null ? Set.of() : serialization.<V>deserializeValueSet(raw)
            );
        }
    }

    private final class TransactionalZSetOperations implements AsyncTransactionZSetOperations<K, V> {

        @Override
        public AsyncTransactionCommand<Boolean> add(K key, V value, double score) {
            return queue(
                    () -> commands.zadd(serialization.serializeKey(key), score, serialization.serializeValue(value)),
                    added -> added != null && added > 0
            );
        }

        @Override
        public AsyncTransactionCommand<Long> add(K key, Set<ZSetOperations.TypedTuple<V>> tuples) {
            List<ScoredValue<byte[]>> scoredValues = serialization.serializeTypedTuples(tuples);
            return queue(
                    () -> commands.zadd(serialization.serializeKey(key), scoredValues.toArray(ScoredValue[]::new)),
                    added -> added == null ? 0L : added
            );
        }

        @Override
        public AsyncTransactionCommand<Long> remove(K key, Object... values) {
            return queue(
                    () -> commands.zrem(serialization.serializeKey(key), serialization.serializeValues(List.of(values))),
                    removed -> removed == null ? 0L : removed
            );
        }

        @Override
        public AsyncTransactionCommand<Double> incrementScore(K key, V value, double delta) {
            return queue(
                    () -> commands.zincrby(serialization.serializeKey(key), delta, serialization.serializeValue(value)),
                    updated -> updated == null ? 0D : updated
            );
        }

        @Override
        public AsyncTransactionCommand<Double> score(K key, Object value) {
            return queue(
                    () -> commands.zscore(serialization.serializeKey(key), serialization.serializeValue(value)),
                    updated -> updated == null ? null : updated
            );
        }

        @Override
        public AsyncTransactionCommand<Long> rank(K key, Object value) {
            return queue(
                    () -> commands.zrank(serialization.serializeKey(key), serialization.serializeValue(value)),
                    updated -> updated == null ? null : updated
            );
        }

        @Override
        public AsyncTransactionCommand<Long> reverseRank(K key, Object value) {
            return queue(
                    () -> commands.zrevrank(serialization.serializeKey(key), serialization.serializeValue(value)),
                    updated -> updated == null ? null : updated
            );
        }

        @Override
        public AsyncTransactionCommand<Set<V>> range(K key, long start, long end) {
            return queue(
                    () -> commands.zrange(serialization.serializeKey(key), start, end),
                    raw -> raw == null ? Set.of() : new LinkedHashSet<>(serialization.<V>deserializeValueBytes(raw))
            );
        }

        @Override
        public AsyncTransactionCommand<Set<ZSetOperations.TypedTuple<V>>> rangeWithScores(K key, long start, long end) {
            return queue(
                    () -> commands.zrangeWithScores(serialization.serializeKey(key), start, end),
                    raw -> raw == null ? Set.of() : serialization.<V>deserializeTypedTuples(raw)
            );
        }

        @Override
        public AsyncTransactionCommand<Long> size(K key) {
            return queue(
                    () -> commands.zcard(serialization.serializeKey(key)),
                    size -> size == null ? 0L : size
            );
        }
    }

    private final class Recorder {

        private final List<Function<Object, Object>> decoders = new ArrayList<>();

        private <R, T> AsyncTransactionCommand<T> queue(
                Supplier<RedisFuture<R>> invocation,
                Function<R, T> decoder
        ) {
            invocation.get();
            int index = decoders.size();
            decoders.add(raw -> decoder.apply((R) raw));
            return new AsyncTransactionCommand<>(index);
        }

        private AsyncTransactionResult decode(TransactionResult transactionResult) {
            if (transactionResult == null) {
                return new AsyncTransactionResult(false, List.of());
            }
            if (transactionResult.wasDiscarded()) {
                return new AsyncTransactionResult(true, List.of());
            }
            if (transactionResult.size() != decoders.size()) {
                throw new IllegalStateException(
                        "Transaction result size mismatch. expected=" + decoders.size() + ", actual=" + transactionResult.size());
            }
            List<Object> results = new ArrayList<>(transactionResult.size());
            for (int i = 0; i < transactionResult.size(); i++) {
                results.add(decoders.get(i).apply(transactionResult.get(i)));
            }
            return new AsyncTransactionResult(false, results);
        }
    }
}
