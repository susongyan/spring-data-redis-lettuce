package com.example.redis.async.connection;

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public interface AsyncRedisConnectionProvider {

    RedisClusterAsyncCommands<byte[], byte[]> commands();
}
