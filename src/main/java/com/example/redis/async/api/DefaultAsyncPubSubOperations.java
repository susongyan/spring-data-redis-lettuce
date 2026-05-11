package com.example.redis.async.api;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.connection.AsyncRedisPubSubSession;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.executor.CommandDescriptor;
import com.example.redis.async.executor.RedisDataStructure;
import com.example.redis.async.serialize.RedisSerializationFacade;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;

final class DefaultAsyncPubSubOperations<K, V> extends AbstractAsyncOperationsSupport implements AsyncPubSubOperations<K, V> {

    DefaultAsyncPubSubOperations(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor,
            RedisSerializationFacade serialization
    ) {
        super(
                Objects.requireNonNull(connectionProvider, "connectionProvider must not be null"),
                Objects.requireNonNull(commandExecutor, "commandExecutor must not be null"),
                Objects.requireNonNull(serialization, "serialization must not be null")
        );
    }

    @Override
    public CompletionStage<Long> publish(K channel, V message) {
        return executeShared(
                new CommandDescriptor("PUBLISH", RedisDataStructure.PUBSUB, 1),
                () -> connectionProvider.commands().publish(
                        serialization.serializeKey(channel),
                        serialization.serializeValue(message)
                ),
                delivered -> delivered == null ? 0L : delivered
        );
    }

    @Override
    public CompletionStage<Subscription> subscribe(Listener<K, V> listener, K... channels) {
        return openSubscription(listener, false, channels);
    }

    @Override
    public CompletionStage<Subscription> psubscribe(Listener<K, V> listener, K... patterns) {
        return openSubscription(listener, true, patterns);
    }

    private CompletionStage<Subscription> openSubscription(Listener<K, V> listener, boolean patternMode, K... targets) {
        return guard(() -> {
            AsyncRedisPubSubSession session = connectionProvider.openPubSubSession();
            RedisPubSubListener<byte[], byte[]> adapter = new ListenerAdapter<>(listener, serialization);
            session.addListener(adapter);
            byte[][] rawTargets = serialization.serializeKeys(java.util.List.of(targets));
            CompletionStage<Void> subscribeStage = commandExecutor.execute(
                    new CommandDescriptor(patternMode ? "PSUBSCRIBE" : "SUBSCRIBE", RedisDataStructure.PUBSUB, rawTargets.length),
                    () -> patternMode ? session.psubscribe(rawTargets) : session.subscribe(rawTargets),
                    ignored -> null
            );
            CompletableFuture<Subscription> result = new CompletableFuture<>();
            subscribeStage.whenComplete((value, error) -> {
                if (error != null) {
                    try {
                        session.removeListener(adapter);
                    } finally {
                        session.close();
                    }
                    result.completeExceptionally(error);
                    return;
                }
                result.complete(new DefaultSubscription(session, adapter, patternMode));
            });
            return result;
        });
    }

    private final class DefaultSubscription implements Subscription {

        private final AsyncRedisPubSubSession session;
        private final RedisPubSubListener<byte[], byte[]> adapter;
        private final boolean patternMode;

        private DefaultSubscription(
                AsyncRedisPubSubSession session,
                RedisPubSubListener<byte[], byte[]> adapter,
                boolean patternMode
        ) {
            this.session = session;
            this.adapter = adapter;
            this.patternMode = patternMode;
        }

        @Override
        public CompletionStage<Void> unsubscribe() {
            CompletionStage<Void> stage = commandExecutor.execute(
                    new CommandDescriptor(patternMode ? "PUNSUBSCRIBE" : "UNSUBSCRIBE", RedisDataStructure.PUBSUB, 0),
                    () -> patternMode ? session.punsubscribe() : session.unsubscribe(),
                    ignored -> null
            );
            CompletableFuture<Void> result = new CompletableFuture<>();
            stage.whenComplete((value, error) -> {
                try {
                    session.removeListener(adapter);
                } finally {
                    session.close();
                }
                if (error != null) {
                    result.completeExceptionally(error);
                    return;
                }
                result.complete(null);
            });
            return result;
        }

        @Override
        public void close() {
            session.removeListener(adapter);
            session.close();
        }
    }

    private static final class ListenerAdapter<K, V> extends RedisPubSubAdapter<byte[], byte[]> {

        private final Listener<K, V> delegate;
        private final RedisSerializationFacade serialization;

        private ListenerAdapter(Listener<K, V> delegate, RedisSerializationFacade serialization) {
            this.delegate = delegate;
            this.serialization = serialization;
        }

        @Override
        public void message(byte[] channel, byte[] message) {
            delegate.message(serialization.deserializeKey(channel), serialization.deserializeValue(message));
        }

        @Override
        public void message(byte[] pattern, byte[] channel, byte[] message) {
            delegate.message(
                    serialization.deserializeKey(pattern),
                    serialization.deserializeKey(channel),
                    serialization.deserializeValue(message)
            );
        }

        @Override
        public void subscribed(byte[] channel, long count) {
            delegate.subscribed(serialization.deserializeKey(channel), count);
        }

        @Override
        public void psubscribed(byte[] pattern, long count) {
            delegate.psubscribed(serialization.deserializeKey(pattern), count);
        }

        @Override
        public void unsubscribed(byte[] channel, long count) {
            delegate.unsubscribed(serialization.deserializeKey(channel), count);
        }

        @Override
        public void punsubscribed(byte[] pattern, long count) {
            delegate.punsubscribed(serialization.deserializeKey(pattern), count);
        }
    }
}
