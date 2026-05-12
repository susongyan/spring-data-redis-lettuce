package com.zuomaigai.redis.async.exception;

import org.springframework.dao.QueryTimeoutException;

public class AsyncRedisCommandTimeoutException extends QueryTimeoutException {

    public AsyncRedisCommandTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
