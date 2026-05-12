package com.zuomaigai.redis.async.exception;

import java.net.ConnectException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.zuomaigai.redis.async.executor.CommandDescriptor;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.netty.handler.codec.CodecException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.serializer.SerializationException;

public class AsyncRedisExceptionTranslator {

    public RuntimeException translate(CommandDescriptor descriptor, Throwable throwable) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        Objects.requireNonNull(throwable, "throwable must not be null");

        Throwable cause = unwrap(throwable);
        String message = "Async Redis command failed: " + descriptor.summary();

        if (cause instanceof RuntimeException runtimeException && !(runtimeException instanceof DataAccessException)) {
            if (runtimeException instanceof SerializationException) {
                return runtimeException;
            }
            if (runtimeException instanceof AsyncRedisUnsupportedOperationException) {
                return runtimeException;
            }
        }

        if (cause instanceof DataAccessException dataAccessException) {
            return dataAccessException;
        }
        if (cause instanceof IllegalArgumentException illegalArgumentException) {
            return new InvalidDataAccessApiUsageException(message, illegalArgumentException);
        }
        if (cause instanceof CodecException codecException) {
            return new SerializationException(message, codecException);
        }
        if (cause instanceof RedisCommandTimeoutException timeoutException) {
            return new AsyncRedisCommandTimeoutException(message, timeoutException);
        }
        if (cause instanceof RedisConnectionException || cause instanceof ConnectException) {
            return new RedisConnectionFailureException(message, cause);
        }
        if (cause instanceof RedisException redisException) {
            return new RedisSystemException(message, redisException);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RedisSystemException(message, cause);
    }

    private Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }
        return current;
    }
}
