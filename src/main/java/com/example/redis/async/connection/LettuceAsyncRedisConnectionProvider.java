package com.example.redis.async.connection;

import java.util.Objects;

import com.example.redis.async.exception.AsyncRedisUnsupportedOperationException;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

public final class LettuceAsyncRedisConnectionProvider implements AsyncRedisConnectionProvider, DisposableBean {

    private final LettuceConnectionFactory connectionFactory;

    private volatile StatefulConnection<byte[], byte[]> connection;
    private volatile RedisClusterAsyncCommands<byte[], byte[]> commands;

    public LettuceAsyncRedisConnectionProvider(LettuceConnectionFactory connectionFactory) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
    }

    @Override
    public RedisClusterAsyncCommands<byte[], byte[]> commands() {
        RedisClusterAsyncCommands<byte[], byte[]> current = commands;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (commands == null) {
                initialize();
            }
            return commands;
        }
    }

    @Override
    public void destroy() {
        StatefulConnection<byte[], byte[]> current = connection;
        if (current != null) {
            current.close();
        }
    }

    private void initialize() {
        AbstractRedisClient nativeClient = connectionFactory.getRequiredNativeClient();
        if (nativeClient instanceof RedisClient redisClient) {
            StatefulRedisConnection<byte[], byte[]> standaloneConnection = redisClient.connect(ByteArrayCodec.INSTANCE);
            this.connection = standaloneConnection;
            this.commands = standaloneConnection.async();
            return;
        }
        if (nativeClient instanceof RedisClusterClient redisClusterClient) {
            StatefulRedisClusterConnection<byte[], byte[]> clusterConnection = redisClusterClient.connect(ByteArrayCodec.INSTANCE);
            this.connection = clusterConnection;
            this.commands = clusterConnection.async();
            return;
        }
        throw new AsyncRedisUnsupportedOperationException(
                "Unsupported Lettuce native client type: " + nativeClient.getClass().getName());
    }
}
