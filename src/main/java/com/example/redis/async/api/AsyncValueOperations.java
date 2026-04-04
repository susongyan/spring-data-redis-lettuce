package com.example.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface AsyncValueOperations<K, V> {

    CompletionStage<V> get(K key);

    CompletionStage<Void> set(K key, V value);

    CompletionStage<Void> set(K key, V value, Duration ttl);

    CompletionStage<Boolean> setIfAbsent(K key, V value);

    CompletionStage<Boolean> setIfPresent(K key, V value);

    CompletionStage<V> getAndSet(K key, V value);

    CompletionStage<List<V>> multiGet(Collection<K> keys);

    CompletionStage<Void> multiSet(Map<K, V> map);

    CompletionStage<Long> increment(K key);

    CompletionStage<Long> increment(K key, long delta);

    CompletionStage<Long> decrement(K key);

    CompletionStage<Long> decrement(K key, long delta);
}
