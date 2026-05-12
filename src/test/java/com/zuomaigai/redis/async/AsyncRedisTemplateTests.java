package com.zuomaigai.redis.async;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.zuomaigai.redis.async.api.AsyncRedisTemplate;
import com.zuomaigai.redis.async.connection.AsyncRedisConnectionSession;
import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import com.zuomaigai.redis.async.connection.AsyncRedisPubSubSession;
import com.zuomaigai.redis.async.exception.AsyncRedisCommandTimeoutException;
import com.zuomaigai.redis.async.exception.AsyncRedisExceptionTranslator;
import com.zuomaigai.redis.async.executor.AsyncCommandExecutor;
import com.zuomaigai.redis.async.executor.LettuceAsyncCommandExecutor;
import com.zuomaigai.redis.async.metrics.NoopAsyncRedisMetricsRecorder;
import com.zuomaigai.redis.async.serialize.RedisTemplateSerializationContext;
import com.zuomaigai.redis.async.support.AsyncRedisTemplateOptions;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncRedisTemplateTests {

    @Test
    void valueGetShouldReuseRedisTemplateSerializer() throws Exception {
        RedisClusterAsyncCommands<byte[], byte[]> commands = mock(RedisClusterAsyncCommands.class);
        when(commands.get(any())).thenReturn(StubRedisFuture.completed(bytes("alice")));

        AsyncRedisTemplate<String, String> template = template(commands);

        String value = template.opsForValue().get("user:1").toCompletableFuture().get(1, TimeUnit.SECONDS);
        assertThat(value).isEqualTo("alice");

        ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(commands).get(keyCaptor.capture());
        assertThat(new String(keyCaptor.getValue(), StandardCharsets.UTF_8)).isEqualTo("user:1");
    }

    @Test
    void hashEntriesShouldDeserializeKeysAndValues() throws Exception {
        RedisClusterAsyncCommands<byte[], byte[]> commands = mock(RedisClusterAsyncCommands.class);
        Map<byte[], byte[]> rawEntries = new LinkedHashMap<>();
        rawEntries.put(bytes("name"), bytes("alice"));
        rawEntries.put(bytes("city"), bytes("shanghai"));
        when(commands.hgetall(any())).thenReturn(StubRedisFuture.completed(rawEntries));

        AsyncRedisTemplate<String, String> template = template(commands);

        Map<String, String> values = template.<String, String>opsForHash()
                .entries("profile:1")
                .toCompletableFuture()
                .get(1, TimeUnit.SECONDS);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("name", "alice");
        expected.put("city", "shanghai");
        assertThat(values).containsExactlyEntriesOf(expected);
    }

    @Test
    void lettuceTimeoutShouldBeTranslated() {
        RedisClusterAsyncCommands<byte[], byte[]> commands = mock(RedisClusterAsyncCommands.class);
        when(commands.get(any())).thenReturn(StubRedisFuture.failed(new RedisCommandTimeoutException("timeout")));

        AsyncRedisTemplate<String, String> template = template(commands);

        assertThatThrownBy(() -> template.opsForValue().get("user:1").toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(AsyncRedisCommandTimeoutException.class);
    }

    private AsyncRedisTemplate<String, String> template(RedisClusterAsyncCommands<byte[], byte[]> commands) {
        AsyncRedisConnectionProvider provider = new AsyncRedisConnectionProvider() {
            @Override
            public RedisClusterAsyncCommands<byte[], byte[]> commands() {
                return commands;
            }

            @Override
            public AsyncRedisConnectionSession openSession() {
                throw new UnsupportedOperationException("Dedicated sessions are not required for this unit test");
            }

            @Override
            public AsyncRedisPubSubSession openPubSubSession() {
                throw new UnsupportedOperationException("Pub/Sub sessions are not required for this unit test");
            }

            @Override
            public boolean isClusterConnection() {
                return false;
            }
        };
        AsyncCommandExecutor executor = new LettuceAsyncCommandExecutor(
                new AsyncRedisExceptionTranslator(),
                NoopAsyncRedisMetricsRecorder.INSTANCE,
                AsyncRedisTemplateOptions.builder().build());
        return new AsyncRedisTemplate<>(provider, executor, RedisTemplateSerializationContext.from(redisTemplate()));
    }

    private RedisTemplate<String, String> redisTemplate() {
        RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        StringRedisSerializer serializer = new StringRedisSerializer();
        redisTemplate.setKeySerializer(serializer);
        redisTemplate.setValueSerializer(serializer);
        redisTemplate.setHashKeySerializer(serializer);
        redisTemplate.setHashValueSerializer(serializer);
        return redisTemplate;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class StubRedisFuture<T> extends CompletableFuture<T> implements RedisFuture<T> {

        static <T> StubRedisFuture<T> completed(T value) {
            StubRedisFuture<T> future = new StubRedisFuture<>();
            future.complete(value);
            return future;
        }

        static <T> StubRedisFuture<T> failed(Throwable error) {
            StubRedisFuture<T> future = new StubRedisFuture<>();
            future.completeExceptionally(error);
            return future;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) {
            return isDone();
        }

        @Override
        public String getError() {
            return null;
        }
    }
}
