package com.zuomaigai.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public interface AsyncRedisOperations<K, V> {

    AsyncValueOperations<K, V> opsForValue();

    <HK, HV> AsyncHashOperations<K, HK, HV> opsForHash();

    AsyncListOperations<K, V> opsForList();

    AsyncSetOperations<K, V> opsForSet();

    AsyncZSetOperations<K, V> opsForZSet();

    <HK, HV> AsyncStreamOperations<K, HK, HV> opsForStream();

    AsyncPubSubOperations<K, V> opsForPubSub();

    CompletionStage<Boolean> delete(K key);

    CompletionStage<Long> delete(Collection<K> keys);

    CompletionStage<Boolean> hasKey(K key);

    CompletionStage<Boolean> expire(K key, Duration timeout);

    CompletionStage<Boolean> persist(K key);

    CompletionStage<Long> getExpire(K key);

    CompletionStage<Long> getExpire(K key, TimeUnit unit);

    CompletionStage<AsyncTransactionOperations.AsyncTransactionResult> executeTransaction(
            Consumer<AsyncTransactionOperations<K, V>> callback
    );

    CompletionStage<AsyncTransactionOperations.AsyncTransactionResult> executeTransaction(
            Collection<K> watchKeys,
            Consumer<AsyncTransactionOperations<K, V>> callback
    );

    <T> CompletionStage<AsyncTransactionOperations.AsyncTransactionExecution<T>> executeTransactionWithResult(
            Function<AsyncTransactionOperations<K, V>, T> callback
    );

    <T> CompletionStage<AsyncTransactionOperations.AsyncTransactionExecution<T>> executeTransactionWithResult(
            Collection<K> watchKeys,
            Function<AsyncTransactionOperations<K, V>, T> callback
    );
}
