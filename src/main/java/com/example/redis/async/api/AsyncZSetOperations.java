package com.example.redis.async.api;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.springframework.data.redis.core.ZSetOperations;

public interface AsyncZSetOperations<K, V> {

    CompletionStage<Boolean> add(K key, V value, double score);

    CompletionStage<Long> add(K key, Set<ZSetOperations.TypedTuple<V>> tuples);

    CompletionStage<Long> remove(K key, Object... values);

    CompletionStage<Double> incrementScore(K key, V value, double delta);

    CompletionStage<Double> score(K key, Object value);

    CompletionStage<Long> rank(K key, Object value);

    CompletionStage<Long> reverseRank(K key, Object value);

    CompletionStage<Set<V>> range(K key, long start, long end);

    CompletionStage<Set<ZSetOperations.TypedTuple<V>>> rangeWithScores(K key, long start, long end);

    CompletionStage<Long> size(K key);
}
