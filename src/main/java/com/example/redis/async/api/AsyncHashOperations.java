package com.example.redis.async.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public interface AsyncHashOperations<K, HK, HV> {

    CompletionStage<HV> get(K key, HK hashKey);

    CompletionStage<Boolean> hasKey(K key, HK hashKey);

    CompletionStage<Void> put(K key, HK hashKey, HV value);

    CompletionStage<Boolean> putIfAbsent(K key, HK hashKey, HV value);

    CompletionStage<Void> putAll(K key, Map<HK, HV> map);

    CompletionStage<List<HV>> multiGet(K key, Collection<HK> hashKeys);

    CompletionStage<Long> delete(K key, Object... hashKeys);

    CompletionStage<Map<HK, HV>> entries(K key);

    CompletionStage<Set<HK>> keys(K key);

    CompletionStage<List<HV>> values(K key);

    CompletionStage<Long> size(K key);

    CompletionStage<Long> increment(K key, HK hashKey, long delta);

    CompletionStage<Double> increment(K key, HK hashKey, double delta);
}
