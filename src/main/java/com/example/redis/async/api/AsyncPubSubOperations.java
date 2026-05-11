package com.example.redis.async.api;

import java.util.concurrent.CompletionStage;

public interface AsyncPubSubOperations<K, V> {

    CompletionStage<Long> publish(K channel, V message);

    CompletionStage<Subscription> subscribe(Listener<K, V> listener, K... channels);

    CompletionStage<Subscription> psubscribe(Listener<K, V> listener, K... patterns);

    interface Listener<K, V> {

        default void message(K channel, V message) {
        }

        default void message(K pattern, K channel, V message) {
        }

        default void subscribed(K channel, long count) {
        }

        default void psubscribed(K pattern, long count) {
        }

        default void unsubscribed(K channel, long count) {
        }

        default void punsubscribed(K pattern, long count) {
        }
    }

    interface Subscription extends AutoCloseable {

        CompletionStage<Void> unsubscribe();

        @Override
        void close();
    }
}
