package com.zuomaigai.redis.async.connection;

import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public interface AsyncRedisConnectionSession extends AutoCloseable {

    RedisClusterAsyncCommands<byte[], byte[]> commands();

    boolean supportsTransactions();

    RedisAsyncCommands<byte[], byte[]> transactionalCommands();

    @Override
    void close();
}
