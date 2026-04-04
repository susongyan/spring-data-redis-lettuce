package com.example.redis.async.serialize;

import java.util.Objects;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

public record RedisTemplateSerializationContext(
        RedisSerializer<Object> keySerializer,
        RedisSerializer<Object> valueSerializer,
        RedisSerializer<Object> hashKeySerializer,
        RedisSerializer<Object> hashValueSerializer
) {

    public RedisTemplateSerializationContext {
        Objects.requireNonNull(keySerializer, "keySerializer must not be null");
        Objects.requireNonNull(valueSerializer, "valueSerializer must not be null");
        Objects.requireNonNull(hashKeySerializer, "hashKeySerializer must not be null");
        Objects.requireNonNull(hashValueSerializer, "hashValueSerializer must not be null");
    }

    public static RedisTemplateSerializationContext from(RedisTemplate<?, ?> redisTemplate) {
        Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");

        RedisSerializer<Object> defaultSerializer = cast(redisTemplate.getDefaultSerializer());
        return new RedisTemplateSerializationContext(
                resolve(redisTemplate.getKeySerializer(), defaultSerializer, "key"),
                resolve(redisTemplate.getValueSerializer(), defaultSerializer, "value"),
                resolve(redisTemplate.getHashKeySerializer(), defaultSerializer, "hashKey"),
                resolve(redisTemplate.getHashValueSerializer(), defaultSerializer, "hashValue"));
    }

    private static RedisSerializer<Object> resolve(
            RedisSerializer<?> serializer,
            RedisSerializer<Object> defaultSerializer,
            String label
    ) {
        RedisSerializer<Object> resolved = cast(serializer);
        if (resolved != null) {
            return resolved;
        }
        if (defaultSerializer != null) {
            return defaultSerializer;
        }
        throw new IllegalStateException("No RedisSerializer configured for " + label);
    }

    @SuppressWarnings("unchecked")
    private static RedisSerializer<Object> cast(RedisSerializer<?> serializer) {
        return (RedisSerializer<Object>) serializer;
    }
}
