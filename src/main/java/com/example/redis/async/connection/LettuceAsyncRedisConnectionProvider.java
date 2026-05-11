package com.example.redis.async.connection;

import java.util.Objects;

import com.example.redis.async.exception.AsyncRedisUnsupportedOperationException;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.async.RedisClusterPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

public final class LettuceAsyncRedisConnectionProvider implements AsyncRedisConnectionProvider, DisposableBean {

    private final LettuceConnectionFactory connectionFactory;

    private volatile StatefulConnection<byte[], byte[]> connection;
    private volatile RedisClusterAsyncCommands<byte[], byte[]> commands;
    private volatile Boolean clusterConnection;

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
    public AsyncRedisConnectionSession openSession() {
        AbstractRedisClient nativeClient = connectionFactory.getRequiredNativeClient();
        if (nativeClient instanceof RedisClient redisClient) {
            StatefulRedisConnection<byte[], byte[]> standaloneConnection = redisClient.connect(ByteArrayCodec.INSTANCE);
            return new StandaloneRedisConnectionSession(standaloneConnection);
        }
        if (nativeClient instanceof RedisClusterClient redisClusterClient) {
            StatefulRedisClusterConnection<byte[], byte[]> clusterConnection = redisClusterClient.connect(ByteArrayCodec.INSTANCE);
            return new ClusterRedisConnectionSession(clusterConnection);
        }
        throw unsupportedClientType(nativeClient);
    }

    @Override
    public AsyncRedisPubSubSession openPubSubSession() {
        AbstractRedisClient nativeClient = connectionFactory.getRequiredNativeClient();
        if (nativeClient instanceof RedisClient redisClient) {
            StatefulRedisPubSubConnection<byte[], byte[]> pubSubConnection = redisClient.connectPubSub(ByteArrayCodec.INSTANCE);
            return new StandaloneRedisPubSubSession(pubSubConnection);
        }
        if (nativeClient instanceof RedisClusterClient redisClusterClient) {
            StatefulRedisClusterPubSubConnection<byte[], byte[]> pubSubConnection =
                    redisClusterClient.connectPubSub(ByteArrayCodec.INSTANCE);
            return new ClusterRedisPubSubSession(pubSubConnection);
        }
        throw unsupportedClientType(nativeClient);
    }

    @Override
    public boolean isClusterConnection() {
        Boolean current = clusterConnection;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (clusterConnection == null) {
                clusterConnection = connectionFactory.getRequiredNativeClient() instanceof RedisClusterClient;
            }
            return clusterConnection;
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
            this.clusterConnection = false;
            return;
        }
        if (nativeClient instanceof RedisClusterClient redisClusterClient) {
            StatefulRedisClusterConnection<byte[], byte[]> clusterConnection = redisClusterClient.connect(ByteArrayCodec.INSTANCE);
            this.connection = clusterConnection;
            this.commands = clusterConnection.async();
            this.clusterConnection = true;
            return;
        }
        throw unsupportedClientType(nativeClient);
    }

    private AsyncRedisUnsupportedOperationException unsupportedClientType(AbstractRedisClient nativeClient) {
        return new AsyncRedisUnsupportedOperationException(
                "Unsupported Lettuce native client type: " + nativeClient.getClass().getName());
    }

    private static final class StandaloneRedisConnectionSession implements AsyncRedisConnectionSession {

        private final StatefulRedisConnection<byte[], byte[]> connection;
        private final RedisAsyncCommands<byte[], byte[]> commands;

        private StandaloneRedisConnectionSession(StatefulRedisConnection<byte[], byte[]> connection) {
            this.connection = connection;
            this.commands = connection.async();
        }

        @Override
        public RedisClusterAsyncCommands<byte[], byte[]> commands() {
            return commands;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public RedisAsyncCommands<byte[], byte[]> transactionalCommands() {
            return commands;
        }

        @Override
        public void close() {
            connection.close();
        }
    }

    private static final class ClusterRedisConnectionSession implements AsyncRedisConnectionSession {

        private final StatefulRedisClusterConnection<byte[], byte[]> connection;
        private final RedisClusterAsyncCommands<byte[], byte[]> commands;

        private ClusterRedisConnectionSession(StatefulRedisClusterConnection<byte[], byte[]> connection) {
            this.connection = connection;
            this.commands = connection.async();
        }

        @Override
        public RedisClusterAsyncCommands<byte[], byte[]> commands() {
            return commands;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public RedisAsyncCommands<byte[], byte[]> transactionalCommands() {
            throw new AsyncRedisUnsupportedOperationException("Transactions are not supported for Redis cluster connections");
        }

        @Override
        public void close() {
            connection.close();
        }
    }

    private static final class StandaloneRedisPubSubSession implements AsyncRedisPubSubSession {

        private final StatefulRedisPubSubConnection<byte[], byte[]> connection;
        private final RedisPubSubAsyncCommands<byte[], byte[]> commands;

        private StandaloneRedisPubSubSession(StatefulRedisPubSubConnection<byte[], byte[]> connection) {
            this.connection = connection;
            this.commands = connection.async();
        }

        @Override
        public void addListener(RedisPubSubListener<byte[], byte[]> listener) {
            connection.addListener(listener);
        }

        @Override
        public void removeListener(RedisPubSubListener<byte[], byte[]> listener) {
            connection.removeListener(listener);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> subscribe(byte[]... channels) {
            return commands.subscribe(channels);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> psubscribe(byte[]... patterns) {
            return commands.psubscribe(patterns);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> unsubscribe(byte[]... channels) {
            return commands.unsubscribe(channels);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> punsubscribe(byte[]... patterns) {
            return commands.punsubscribe(patterns);
        }

        @Override
        public void close() {
            connection.close();
        }
    }

    private static final class ClusterRedisPubSubSession implements AsyncRedisPubSubSession {

        private final StatefulRedisClusterPubSubConnection<byte[], byte[]> connection;
        private final RedisClusterPubSubAsyncCommands<byte[], byte[]> commands;

        private ClusterRedisPubSubSession(StatefulRedisClusterPubSubConnection<byte[], byte[]> connection) {
            this.connection = connection;
            this.commands = connection.async();
        }

        @Override
        public void addListener(RedisPubSubListener<byte[], byte[]> listener) {
            connection.addListener(listener);
        }

        @Override
        public void removeListener(RedisPubSubListener<byte[], byte[]> listener) {
            connection.removeListener(listener);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> subscribe(byte[]... channels) {
            return commands.subscribe(channels);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> psubscribe(byte[]... patterns) {
            return commands.psubscribe(patterns);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> unsubscribe(byte[]... channels) {
            return commands.unsubscribe(channels);
        }

        @Override
        public io.lettuce.core.RedisFuture<Void> punsubscribe(byte[]... patterns) {
            return commands.punsubscribe(patterns);
        }

        @Override
        public void close() {
            connection.close();
        }
    }
}
