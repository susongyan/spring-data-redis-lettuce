package com.example.redis.async.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.ZSetOperations;

public interface AsyncTransactionOperations<K, V> {

    AsyncTransactionValueOperations<K, V> opsForValue();

    <HK, HV> AsyncTransactionHashOperations<K, HK, HV> opsForHash();

    AsyncTransactionListOperations<K, V> opsForList();

    AsyncTransactionSetOperations<K, V> opsForSet();

    AsyncTransactionZSetOperations<K, V> opsForZSet();

    AsyncTransactionCommand<Boolean> delete(K key);

    AsyncTransactionCommand<Long> delete(Collection<K> keys);

    AsyncTransactionCommand<Boolean> hasKey(K key);

    AsyncTransactionCommand<Boolean> expire(K key, Duration timeout);

    AsyncTransactionCommand<Boolean> persist(K key);

    interface AsyncTransactionValueOperations<K, V> {

        AsyncTransactionCommand<V> get(K key);

        AsyncTransactionCommand<Void> set(K key, V value);

        AsyncTransactionCommand<Void> set(K key, V value, Duration ttl);

        AsyncTransactionCommand<Boolean> setIfAbsent(K key, V value);

        AsyncTransactionCommand<Boolean> setIfPresent(K key, V value);

        AsyncTransactionCommand<V> getAndSet(K key, V value);

        AsyncTransactionCommand<List<V>> multiGet(Collection<K> keys);

        AsyncTransactionCommand<Void> multiSet(Map<K, V> map);

        AsyncTransactionCommand<Long> increment(K key);

        AsyncTransactionCommand<Long> increment(K key, long delta);

        AsyncTransactionCommand<Long> decrement(K key);

        AsyncTransactionCommand<Long> decrement(K key, long delta);
    }

    interface AsyncTransactionHashOperations<K, HK, HV> {

        AsyncTransactionCommand<HV> get(K key, HK hashKey);

        AsyncTransactionCommand<Boolean> hasKey(K key, HK hashKey);

        AsyncTransactionCommand<Void> put(K key, HK hashKey, HV value);

        AsyncTransactionCommand<Boolean> putIfAbsent(K key, HK hashKey, HV value);

        AsyncTransactionCommand<Void> putAll(K key, Map<HK, HV> map);

        AsyncTransactionCommand<List<HV>> multiGet(K key, Collection<HK> hashKeys);

        AsyncTransactionCommand<Long> delete(K key, Object... hashKeys);

        AsyncTransactionCommand<Map<HK, HV>> entries(K key);

        AsyncTransactionCommand<Set<HK>> keys(K key);

        AsyncTransactionCommand<List<HV>> values(K key);

        AsyncTransactionCommand<Long> size(K key);

        AsyncTransactionCommand<Long> increment(K key, HK hashKey, long delta);

        AsyncTransactionCommand<Double> increment(K key, HK hashKey, double delta);
    }

    interface AsyncTransactionListOperations<K, V> {

        AsyncTransactionCommand<List<V>> range(K key, long start, long end);

        AsyncTransactionCommand<Void> trim(K key, long start, long end);

        AsyncTransactionCommand<Long> size(K key);

        AsyncTransactionCommand<Long> leftPush(K key, V value);

        AsyncTransactionCommand<Long> leftPushAll(K key, Collection<V> values);

        AsyncTransactionCommand<Long> rightPush(K key, V value);

        AsyncTransactionCommand<Long> rightPushAll(K key, Collection<V> values);

        AsyncTransactionCommand<V> leftPop(K key);

        AsyncTransactionCommand<V> rightPop(K key);
    }

    interface AsyncTransactionSetOperations<K, V> {

        AsyncTransactionCommand<Long> add(K key, V... values);

        AsyncTransactionCommand<Long> remove(K key, Object... values);

        AsyncTransactionCommand<V> pop(K key);

        AsyncTransactionCommand<Set<V>> pop(K key, long count);

        AsyncTransactionCommand<Boolean> move(K key, V value, K destinationKey);

        AsyncTransactionCommand<Long> size(K key);

        AsyncTransactionCommand<Boolean> isMember(K key, Object value);

        AsyncTransactionCommand<Set<V>> members(K key);
    }

    interface AsyncTransactionZSetOperations<K, V> {

        AsyncTransactionCommand<Boolean> add(K key, V value, double score);

        AsyncTransactionCommand<Long> add(K key, Set<ZSetOperations.TypedTuple<V>> tuples);

        AsyncTransactionCommand<Long> remove(K key, Object... values);

        AsyncTransactionCommand<Double> incrementScore(K key, V value, double delta);

        AsyncTransactionCommand<Double> score(K key, Object value);

        AsyncTransactionCommand<Long> rank(K key, Object value);

        AsyncTransactionCommand<Long> reverseRank(K key, Object value);

        AsyncTransactionCommand<Set<V>> range(K key, long start, long end);

        AsyncTransactionCommand<Set<ZSetOperations.TypedTuple<V>>> rangeWithScores(K key, long start, long end);

        AsyncTransactionCommand<Long> size(K key);
    }

    record AsyncTransactionCommand<T>(int index) {
    }

    final class AsyncTransactionResult {

        private final boolean discarded;
        private final List<Object> results;

        public AsyncTransactionResult(boolean discarded, List<Object> results) {
            this.discarded = discarded;
            this.results = Collections.unmodifiableList(new ArrayList<>(results));
        }

        public boolean discarded() {
            return discarded;
        }

        public List<Object> results() {
            return results;
        }

        @SuppressWarnings("unchecked")
        public <T> T get(int index) {
            return (T) results.get(index);
        }

        public <T> T get(AsyncTransactionCommand<T> command) {
            return get(command.index());
        }
    }

    record AsyncTransactionExecution<T>(T context, AsyncTransactionResult result) {
    }
}
