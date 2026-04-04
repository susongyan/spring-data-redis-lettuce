package com.example.redis.async.executor;

public record CommandDescriptor(
        String command,
        RedisDataStructure dataStructure,
        int keyCount
) {

    public String summary() {
        return command + "[" + dataStructure.name().toLowerCase() + ",keys=" + keyCount + "]";
    }
}
