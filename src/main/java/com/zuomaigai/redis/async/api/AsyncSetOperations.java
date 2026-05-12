package com.zuomaigai.redis.async.api;

import java.util.Set;
import java.util.concurrent.CompletionStage;

public interface AsyncSetOperations<K, V> {

    CompletionStage<Long> add(K key, V... values);

    CompletionStage<Long> remove(K key, Object... values);

    CompletionStage<V> pop(K key);

    CompletionStage<Set<V>> pop(K key, long count);

    CompletionStage<Boolean> move(K key, V value, K destinationKey);

    CompletionStage<Long> size(K key);

    CompletionStage<Boolean> isMember(K key, Object value);

    CompletionStage<Set<V>> members(K key);
}
