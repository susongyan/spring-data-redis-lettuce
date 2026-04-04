package com.example.redis.async.config;

import java.util.Objects;

import com.example.redis.async.api.AsyncRedisTemplate;
import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.serialize.RedisTemplateSerializationContext;
import org.springframework.data.redis.core.RedisTemplate;

public final class AsyncRedisTemplateFactory {

    private final AsyncRedisConnectionProvider connectionProvider;
    private final AsyncCommandExecutor commandExecutor;

    public AsyncRedisTemplateFactory(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor
    ) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor must not be null");
    }

    public <K, V> AsyncRedisTemplate<K, V> create(RedisTemplate<K, V> redisTemplate) {
        return new AsyncRedisTemplate<>(
                connectionProvider,
                commandExecutor,
                RedisTemplateSerializationContext.from(redisTemplate));
    }
}
