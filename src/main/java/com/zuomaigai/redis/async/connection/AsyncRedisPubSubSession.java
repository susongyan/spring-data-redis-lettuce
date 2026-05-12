package com.zuomaigai.redis.async.connection;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.pubsub.RedisPubSubListener;

public interface AsyncRedisPubSubSession extends AutoCloseable {

    void addListener(RedisPubSubListener<byte[], byte[]> listener);

    void removeListener(RedisPubSubListener<byte[], byte[]> listener);

    RedisFuture<Void> subscribe(byte[]... channels);

    RedisFuture<Void> psubscribe(byte[]... patterns);

    RedisFuture<Void> unsubscribe(byte[]... channels);

    RedisFuture<Void> punsubscribe(byte[]... patterns);

    @Override
    void close();
}
