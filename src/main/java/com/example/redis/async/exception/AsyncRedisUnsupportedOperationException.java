package com.example.redis.async.exception;

import org.springframework.dao.InvalidDataAccessApiUsageException;

public class AsyncRedisUnsupportedOperationException extends InvalidDataAccessApiUsageException {

    public AsyncRedisUnsupportedOperationException(String msg) {
        super(msg);
    }
}
