package com.zuomaigai.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface AsyncListOperations<K, V> {

    CompletionStage<List<V>> range(K key, long start, long end);

    CompletionStage<Void> trim(K key, long start, long end);

    CompletionStage<Long> size(K key);

    CompletionStage<Long> leftPush(K key, V value);

    CompletionStage<Long> leftPushAll(K key, Collection<V> values);

    CompletionStage<Long> rightPush(K key, V value);

    CompletionStage<Long> rightPushAll(K key, Collection<V> values);

    CompletionStage<V> leftPop(K key);

    CompletionStage<V> rightPop(K key);

    CompletionStage<BlockingPopResult<K, V>> leftPop(Duration timeout, Collection<K> keys);

    CompletionStage<BlockingPopResult<K, V>> rightPop(Duration timeout, Collection<K> keys);

    record BlockingPopResult<K, V>(K key, V value) {
    }
}
