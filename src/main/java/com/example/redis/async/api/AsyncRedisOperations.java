package com.example.redis.async.api;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public interface AsyncRedisOperations<K, V> {

    AsyncValueOperations<K, V> opsForValue();

    <HK, HV> AsyncHashOperations<K, HK, HV> opsForHash();

    CompletionStage<Boolean> delete(K key);

    CompletionStage<Long> delete(Collection<K> keys);

    CompletionStage<Boolean> hasKey(K key);

    CompletionStage<Boolean> expire(K key, Duration timeout);

    CompletionStage<Boolean> persist(K key);

    CompletionStage<Long> getExpire(K key);

    CompletionStage<Long> getExpire(K key, TimeUnit unit);
}
