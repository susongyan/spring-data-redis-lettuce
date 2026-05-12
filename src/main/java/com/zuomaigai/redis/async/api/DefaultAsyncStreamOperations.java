package com.zuomaigai.redis.async.api;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import com.zuomaigai.redis.async.executor.AsyncCommandExecutor;
import com.zuomaigai.redis.async.executor.CommandDescriptor;
import com.zuomaigai.redis.async.executor.RedisDataStructure;
import com.zuomaigai.redis.async.serialize.RedisSerializationFacade;
import io.lettuce.core.Range;
import io.lettuce.core.XReadArgs;

final class DefaultAsyncStreamOperations<K, HK, HV> extends AbstractAsyncOperationsSupport
        implements AsyncStreamOperations<K, HK, HV> {

    DefaultAsyncStreamOperations(
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
    public CompletionStage<String> add(K key, Map<? extends HK, ? extends HV> body) {
        return executeShared(
                new CommandDescriptor("XADD", RedisDataStructure.STREAM, 1),
                () -> connectionProvider.commands().xadd(
                        serialization.serializeKey(key),
                        serialization.serializeHashMap(body)
                ),
                value -> value
        );
    }

    @Override
    public CompletionStage<Long> acknowledge(K key, String group, String... recordIds) {
        return executeShared(
                new CommandDescriptor("XACK", RedisDataStructure.STREAM, 1),
                () -> connectionProvider.commands().xack(
                        serialization.serializeKey(key),
                        serialization.serializeString(group),
                        recordIds
                ),
                acknowledged -> acknowledged == null ? 0L : acknowledged
        );
    }

    @Override
    public CompletionStage<String> createGroup(K key, String group) {
        return executeShared(
                new CommandDescriptor("XGROUP CREATE", RedisDataStructure.STREAM, 1),
                () -> connectionProvider.commands().xgroupCreate(
                        XReadArgs.StreamOffset.latest(serialization.serializeKey(key)),
                        serialization.serializeString(group)
                ),
                value -> value
        );
    }

    @Override
    public CompletionStage<Boolean> destroyGroup(K key, String group) {
        return executeShared(
                new CommandDescriptor("XGROUP DESTROY", RedisDataStructure.STREAM, 1),
                () -> connectionProvider.commands().xgroupDestroy(
                        serialization.serializeKey(key),
                        serialization.serializeString(group)
                ),
                Boolean.TRUE::equals
        );
    }

    @Override
    public CompletionStage<List<AsyncStreamOperations.StreamMessage<K, HK, HV>>> range(K key, String start, String end) {
        return executeShared(
                new CommandDescriptor("XRANGE", RedisDataStructure.STREAM, 1),
                () -> connectionProvider.commands().xrange(
                        serialization.serializeKey(key),
                        Range.create(start, end)
                ),
                this::decodeMessages
        );
    }

    @Override
    public CompletionStage<List<AsyncStreamOperations.StreamMessage<K, HK, HV>>> read(
            ReadOptions options,
            StreamOffset<K>... streams
    ) {
        ReadOptions readOptions = options == null ? ReadOptions.builder().build() : options;
        io.lettuce.core.XReadArgs.StreamOffset<byte[]>[] offsets = toStreamOffsets(streams);
        XReadArgs args = toReadArgs(readOptions);
        if (isBlocking(readOptions)) {
            return executeDedicated(
                    new CommandDescriptor("XREAD", RedisDataStructure.STREAM, offsets.length),
                    session -> session.commands().xread(args, offsets),
                    this::decodeMessages
            );
        }
        return executeShared(
                new CommandDescriptor("XREAD", RedisDataStructure.STREAM, offsets.length),
                () -> connectionProvider.commands().xread(args, offsets),
                this::decodeMessages
        );
    }

    @Override
    public CompletionStage<List<AsyncStreamOperations.StreamMessage<K, HK, HV>>> read(
            AsyncStreamOperations.Consumer consumer,
            ReadOptions options,
            StreamOffset<K>... streams
    ) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        ReadOptions readOptions = options == null ? ReadOptions.builder().build() : options;
        io.lettuce.core.XReadArgs.StreamOffset<byte[]>[] offsets = toStreamOffsets(streams);
        XReadArgs args = toReadArgs(readOptions);
        io.lettuce.core.Consumer<byte[]> rawConsumer = io.lettuce.core.Consumer.from(
                serialization.serializeString(consumer.group()),
                serialization.serializeString(consumer.name())
        );
        if (isBlocking(readOptions)) {
            return executeDedicated(
                    new CommandDescriptor("XREADGROUP", RedisDataStructure.STREAM, offsets.length),
                    session -> session.commands().xreadgroup(rawConsumer, args, offsets),
                    this::decodeMessages
            );
        }
        return executeShared(
                new CommandDescriptor("XREADGROUP", RedisDataStructure.STREAM, offsets.length),
                () -> connectionProvider.commands().xreadgroup(rawConsumer, args, offsets),
                this::decodeMessages
        );
    }

    @Override
    public CompletionStage<Long> trim(K key, long count) {
        return executeShared(
                new CommandDescriptor("XTRIM", RedisDataStructure.STREAM, 1),
                () -> connectionProvider.commands().xtrim(serialization.serializeKey(key), count),
                trimmed -> trimmed == null ? 0L : trimmed
        );
    }

    private boolean isBlocking(ReadOptions options) {
        Duration block = options.block();
        return block != null && !block.isZero() && !block.isNegative();
    }

    private XReadArgs toReadArgs(ReadOptions options) {
        XReadArgs args = new XReadArgs();
        if (options.block() != null) {
            args.block(options.block().toMillis());
        }
        if (options.count() != null) {
            args.count(options.count());
        }
        if (options.noack()) {
            args.noack(true);
        }
        return args;
    }

    @SuppressWarnings("unchecked")
    private io.lettuce.core.XReadArgs.StreamOffset<byte[]>[] toStreamOffsets(StreamOffset<K>[] streams) {
        io.lettuce.core.XReadArgs.StreamOffset<byte[]>[] offsets = new io.lettuce.core.XReadArgs.StreamOffset[streams.length];
        for (int i = 0; i < streams.length; i++) {
            StreamOffset<K> stream = streams[i];
            offsets[i] = io.lettuce.core.XReadArgs.StreamOffset.from(
                    serialization.serializeKey(stream.key()),
                    stream.offset()
            );
        }
        return offsets;
    }

    private List<AsyncStreamOperations.StreamMessage<K, HK, HV>> decodeMessages(
            List<io.lettuce.core.StreamMessage<byte[], byte[]>> rawMessages
    ) {
        if (rawMessages == null || rawMessages.isEmpty()) {
            return List.of();
        }
        List<AsyncStreamOperations.StreamMessage<K, HK, HV>> messages = new java.util.ArrayList<>(rawMessages.size());
        for (io.lettuce.core.StreamMessage<byte[], byte[]> rawMessage : rawMessages) {
            messages.add(new AsyncStreamOperations.StreamMessage<>(
                    serialization.deserializeKey(rawMessage.getStream()),
                    rawMessage.getId(),
                    serialization.deserializeHashEntries(rawMessage.getBody())
            ));
        }
        return messages;
    }
}
