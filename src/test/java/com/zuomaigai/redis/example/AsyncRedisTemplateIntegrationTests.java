package com.zuomaigai.redis.example;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.zuomaigai.redis.async.api.AsyncListOperations;
import com.zuomaigai.redis.async.api.AsyncPubSubOperations;
import com.zuomaigai.redis.async.api.AsyncRedisTemplate;
import com.zuomaigai.redis.async.api.AsyncStreamOperations;
import com.zuomaigai.redis.async.api.AsyncTransactionOperations;
import com.zuomaigai.redis.async.connection.AsyncRedisConnectionProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import redis.embedded.RedisServer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = AsyncRedisExampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncRedisTemplateIntegrationTests {

    private static final int REDIS_PORT = findAvailableTcpPort();
    private static final RedisServer REDIS_SERVER = buildRedisServer(REDIS_PORT);

    @LocalServerPort
    private int port;

    @Autowired
    @Qualifier("asyncStringRedisTemplate")
    private AsyncRedisTemplate<String, String> asyncRedisTemplate;

    @Autowired
    private AsyncRedisConnectionProvider asyncRedisConnectionProvider;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @DynamicPropertySource
    static void registerRedisProperties(DynamicPropertyRegistry registry) throws IOException {
        if (!REDIS_SERVER.isActive()) {
            REDIS_SERVER.start();
        }
        registry.add("spring.data.redis.host", () -> "127.0.0.1");
        registry.add("spring.data.redis.port", () -> REDIS_PORT);
    }

    @AfterAll
    void stopRedis() throws Exception {
        if (asyncRedisConnectionProvider instanceof DisposableBean disposableBean) {
            disposableBean.destroy();
        }
        if (REDIS_SERVER.isActive()) {
            REDIS_SERVER.stop();
        }
    }

    @Test
    void asyncTemplateShouldReadAndWriteAgainstRealRedis() throws Exception {
        String key = "it:value:" + UUID.randomUUID();
        String hashKey = "it:hash:" + UUID.randomUUID();

        asyncRedisTemplate.opsForValue().set(key, "hello", Duration.ofMinutes(1))
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        String value = asyncRedisTemplate.opsForValue().get(key)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("displayName", "Alice");
        fields.put("city", "Shanghai");
        asyncRedisTemplate.<String, String>opsForHash().putAll(hashKey, fields)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        Map<String, String> entries = asyncRedisTemplate.<String, String>opsForHash()
                .entries(hashKey)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(value).isEqualTo("hello");
        assertThat(entries).containsExactlyEntriesOf(fields);
    }

    @Test
    void httpExampleShouldPersistProfileAndStatusIntoRealRedis() throws Exception {
        String userId = UUID.randomUUID().toString();
        UserProfileUpsertRequest request = new UserProfileUpsertRequest("Alice", "alice@example.com", "Shanghai");

        ResponseEntity<UserProfile> putResponse = testRestTemplate.exchange(
                baseUrl("/api/users/" + userId),
                HttpMethod.PUT,
                new HttpEntity<>(request),
                UserProfile.class
        );

        assertThat(putResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(putResponse.getBody()).isNotNull();
        assertThat(putResponse.getBody().status()).isEqualTo("ACTIVE");

        ResponseEntity<UserProfile> getResponse = testRestTemplate.getForEntity(
                baseUrl("/api/users/" + userId),
                UserProfile.class
        );

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResponse.getBody()).isEqualTo(new UserProfile(
                userId,
                "Alice",
                "alice@example.com",
                "Shanghai",
                "ACTIVE"
        ));

        ResponseEntity<Void> updateStatusResponse = testRestTemplate.exchange(
                baseUrl("/api/users/" + userId + "/status?value=BUSY"),
                HttpMethod.PUT,
                HttpEntity.EMPTY,
                Void.class
        );

        assertThat(updateStatusResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        Map<String, String> profileEntries = asyncRedisTemplate.<String, String>opsForHash()
                .entries("example:user:profile:" + userId)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        String status = asyncRedisTemplate.opsForValue()
                .get("example:user:status:" + userId)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(profileEntries).containsEntry("displayName", "Alice");
        assertThat(profileEntries).containsEntry("email", "alice@example.com");
        assertThat(profileEntries).containsEntry("city", "Shanghai");
        assertThat(status).isEqualTo("BUSY");

        ResponseEntity<Void> deleteResponse = testRestTemplate.exchange(
                baseUrl("/api/users/" + userId),
                HttpMethod.DELETE,
                HttpEntity.EMPTY,
                Void.class
        );

        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        ResponseEntity<UserProfile> afterDeleteResponse = testRestTemplate.getForEntity(
                baseUrl("/api/users/" + userId),
                UserProfile.class
        );

        assertThat(afterDeleteResponse.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void blockingDemoEndpointShouldWaitForAsyncResult() throws Exception {
        String userId = UUID.randomUUID().toString();
        UserProfileUpsertRequest request = new UserProfileUpsertRequest("Bob", "bob@example.com", "Hangzhou");

        ResponseEntity<UserProfile> putResponse = testRestTemplate.exchange(
                baseUrl("/api/users/blocking/" + userId),
                HttpMethod.PUT,
                new HttpEntity<>(request),
                UserProfile.class
        );

        assertThat(putResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(putResponse.getBody()).isEqualTo(new UserProfile(
                userId,
                "Bob",
                "bob@example.com",
                "Hangzhou",
                "ACTIVE"
        ));

        ResponseEntity<UserProfile> getResponse = testRestTemplate.getForEntity(
                baseUrl("/api/users/blocking/" + userId),
                UserProfile.class
        );

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResponse.getBody()).isEqualTo(new UserProfile(
                userId,
                "Bob",
                "bob@example.com",
                "Hangzhou",
                "ACTIVE"
        ));

        String status = asyncRedisTemplate.opsForValue()
                .get("example:user:status:" + userId)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(status).isEqualTo("ACTIVE");
    }

    @Test
    void listSetZSetOperationsShouldWorkAgainstRealRedis() throws Exception {
        String listKey = "it:list:" + UUID.randomUUID();
        String setKey = "it:set:" + UUID.randomUUID();
        String movedSetKey = "it:set:moved:" + UUID.randomUUID();
        String zsetKey = "it:zset:" + UUID.randomUUID();

        Long listLength = await(asyncRedisTemplate.opsForList().rightPushAll(listKey, List.of("one", "two", "three")));
        List<String> listValues = await(asyncRedisTemplate.opsForList().range(listKey, 0, -1));
        String leftPopped = await(asyncRedisTemplate.opsForList().leftPop(listKey));
        Long remainingListSize = await(asyncRedisTemplate.opsForList().size(listKey));

        Long addedSetMembers = await(asyncRedisTemplate.opsForSet().add(setKey, "red", "green", "blue"));
        Boolean moved = await(asyncRedisTemplate.opsForSet().move(setKey, "green", movedSetKey));
        Set<String> sourceMembers = await(asyncRedisTemplate.opsForSet().members(setKey));
        Set<String> destinationMembers = await(asyncRedisTemplate.opsForSet().members(movedSetKey));
        Boolean sourceContainsGreen = await(asyncRedisTemplate.opsForSet().isMember(setKey, "green"));
        Long sourceSetSize = await(asyncRedisTemplate.opsForSet().size(setKey));

        Boolean aliceAdded = await(asyncRedisTemplate.opsForZSet().add(zsetKey, "alice", 1.0D));
        Set<ZSetOperations.TypedTuple<String>> tuples = new LinkedHashSet<>();
        tuples.add(ZSetOperations.TypedTuple.of("bob", 2.0D));
        tuples.add(ZSetOperations.TypedTuple.of("carol", 0.5D));
        Long bulkAdded = await(asyncRedisTemplate.opsForZSet().add(zsetKey, tuples));
        Double aliceScore = await(asyncRedisTemplate.opsForZSet().incrementScore(zsetKey, "alice", 2.5D));
        Set<String> orderedMembers = await(asyncRedisTemplate.opsForZSet().range(zsetKey, 0, -1));
        Set<ZSetOperations.TypedTuple<String>> scoredMembers = await(
                asyncRedisTemplate.opsForZSet().rangeWithScores(zsetKey, 0, -1)
        );
        Long aliceRank = await(asyncRedisTemplate.opsForZSet().rank(zsetKey, "alice"));
        Long aliceReverseRank = await(asyncRedisTemplate.opsForZSet().reverseRank(zsetKey, "alice"));
        Long zsetSize = await(asyncRedisTemplate.opsForZSet().size(zsetKey));

        assertThat(listLength).isEqualTo(3L);
        assertThat(listValues).containsExactly("one", "two", "three");
        assertThat(leftPopped).isEqualTo("one");
        assertThat(remainingListSize).isEqualTo(2L);

        assertThat(addedSetMembers).isEqualTo(3L);
        assertThat(moved).isTrue();
        assertThat(sourceMembers).containsExactlyInAnyOrder("red", "blue");
        assertThat(destinationMembers).containsExactly("green");
        assertThat(sourceContainsGreen).isFalse();
        assertThat(sourceSetSize).isEqualTo(2L);

        assertThat(aliceAdded).isTrue();
        assertThat(bulkAdded).isEqualTo(2L);
        assertThat(aliceScore).isEqualTo(3.5D);
        assertThat(new ArrayList<>(orderedMembers)).containsExactly("carol", "bob", "alice");
        assertThat(new ArrayList<>(scoredMembers))
                .extracting(ZSetOperations.TypedTuple::getValue, ZSetOperations.TypedTuple::getScore)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("carol", 0.5D),
                        org.assertj.core.groups.Tuple.tuple("bob", 2.0D),
                        org.assertj.core.groups.Tuple.tuple("alice", 3.5D)
                );
        assertThat(aliceRank).isEqualTo(2L);
        assertThat(aliceReverseRank).isEqualTo(0L);
        assertThat(zsetSize).isEqualTo(3L);
    }

    @Test
    void transactionShouldQueueAndExecAgainstRealRedis() throws Exception {
        String counterKey = "it:tx:value:" + UUID.randomUUID();
        String hashKey = "it:tx:hash:" + UUID.randomUUID();
        String listKey = "it:tx:list:" + UUID.randomUUID();
        String zsetKey = "it:tx:zset:" + UUID.randomUUID();

        record TxHandles(
                AsyncTransactionOperations.AsyncTransactionCommand<Void> setCounter,
                AsyncTransactionOperations.AsyncTransactionCommand<Long> incrementCounter,
                AsyncTransactionOperations.AsyncTransactionCommand<Void> putProfile,
                AsyncTransactionOperations.AsyncTransactionCommand<Map<String, String>> readProfile,
                AsyncTransactionOperations.AsyncTransactionCommand<Long> pushAuditLog,
                AsyncTransactionOperations.AsyncTransactionCommand<Boolean> addRanking
        ) {
        }

        AsyncTransactionOperations.AsyncTransactionExecution<TxHandles> execution = await(
                asyncRedisTemplate.executeTransactionWithResult(tx -> new TxHandles(
                        tx.opsForValue().set(counterKey, "1"),
                        tx.opsForValue().increment(counterKey, 2),
                        tx.<String, String>opsForHash().put(hashKey, "displayName", "Alice"),
                        tx.<String, String>opsForHash().entries(hashKey),
                        tx.opsForList().rightPush(listKey, "created"),
                        tx.opsForZSet().add(zsetKey, "alice", 10.0D)
                ))
        );

        AsyncTransactionOperations.AsyncTransactionResult result = execution.result();

        assertThat(result.discarded()).isFalse();
        assertThat(result.get(execution.context().setCounter())).isNull();
        assertThat(result.get(execution.context().incrementCounter())).isEqualTo(3L);
        assertThat(result.get(execution.context().putProfile())).isNull();
        assertThat(result.get(execution.context().readProfile())).containsEntry("displayName", "Alice");
        assertThat(result.get(execution.context().pushAuditLog())).isEqualTo(1L);
        assertThat(result.get(execution.context().addRanking())).isTrue();

        assertThat(await(asyncRedisTemplate.opsForValue().get(counterKey))).isEqualTo("3");
        assertThat(await(asyncRedisTemplate.<String, String>opsForHash().entries(hashKey)))
                .containsEntry("displayName", "Alice");
        assertThat(await(asyncRedisTemplate.opsForList().range(listKey, 0, -1))).containsExactly("created");
        assertThat(await(asyncRedisTemplate.opsForZSet().score(zsetKey, "alice"))).isEqualTo(10.0D);
    }

    @Test
    void blockingListPopShouldUseDedicatedConnectionAndReturnWhenValueArrives() throws Exception {
        String listKey = "it:blocking:list:" + UUID.randomUUID();

        CompletionStage<AsyncListOperations.BlockingPopResult<String, String>> popStage =
                asyncRedisTemplate.opsForList().leftPop(Duration.ofSeconds(5), List.of(listKey));

        CompletableFuture<Void> producer = CompletableFuture.runAsync(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
                await(asyncRedisTemplate.opsForList().rightPush(listKey, "payload"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        AsyncListOperations.BlockingPopResult<String, String> popResult = await(popStage);
        producer.get(5, TimeUnit.SECONDS);

        assertThat(popResult).isEqualTo(new AsyncListOperations.BlockingPopResult<>(listKey, "payload"));
    }

    @Test
    void pubSubShouldPublishAndReceiveMessages() throws Exception {
        String channel = "it:pubsub:" + UUID.randomUUID();
        CompletableFuture<String> subscribed = new CompletableFuture<>();
        CompletableFuture<String> messageReceived = new CompletableFuture<>();

        AsyncPubSubOperations.Subscription subscription = await(asyncRedisTemplate.opsForPubSub().subscribe(
                new AsyncPubSubOperations.Listener<>() {
                    @Override
                    public void subscribed(String subscribedChannel, long count) {
                        if (channel.equals(subscribedChannel)) {
                            subscribed.complete(subscribedChannel);
                        }
                    }

                    @Override
                    public void message(String messageChannel, String message) {
                        if (channel.equals(messageChannel)) {
                            messageReceived.complete(message);
                        }
                    }
                },
                channel
        ));

        try {
            assertThat(subscribed.get(5, TimeUnit.SECONDS)).isEqualTo(channel);
            Long delivered = await(asyncRedisTemplate.opsForPubSub().publish(channel, "hello-pubsub"));
            assertThat(delivered).isEqualTo(1L);
            assertThat(messageReceived.get(5, TimeUnit.SECONDS)).isEqualTo("hello-pubsub");
        } finally {
            await(subscription.unsubscribe());
        }
    }

    @Test
    void streamOperationsShouldCreateGroupReadAndAck() throws Exception {
        String streamKey = "it:stream:" + UUID.randomUUID();
        String group = "group-a";
        String consumer = "consumer-1";

        AsyncStreamOperations<String, String, String> streamOperations = asyncRedisTemplate.opsForStream();

        String bootstrapId = await(streamOperations.add(streamKey, Map.of("bootstrap", "true")));
        String createGroupResult = await(streamOperations.createGroup(streamKey, group));
        String messageId = await(streamOperations.add(streamKey, Map.of("event", "created", "user", "alice")));
        List<AsyncStreamOperations.StreamMessage<String, String, String>> rangeMessages =
                await(streamOperations.range(streamKey, "-", "+"));
        List<AsyncStreamOperations.StreamMessage<String, String, String>> consumerMessages = await(
                streamOperations.read(
                        AsyncStreamOperations.Consumer.from(group, consumer),
                        AsyncStreamOperations.ReadOptions.builder().count(1).build(),
                        AsyncStreamOperations.StreamOffset.lastConsumed(streamKey)
                )
        );
        Long acknowledged = await(streamOperations.acknowledge(streamKey, group, messageId));
        Long trimmed = await(streamOperations.trim(streamKey, 1));
        Boolean destroyed = await(streamOperations.destroyGroup(streamKey, group));

        assertThat(bootstrapId).isNotBlank();
        assertThat(createGroupResult).isEqualTo("OK");
        assertThat(rangeMessages).extracting(AsyncStreamOperations.StreamMessage::id)
                .contains(bootstrapId, messageId);
        assertThat(consumerMessages).hasSize(1);
        assertThat(consumerMessages.get(0).id()).isEqualTo(messageId);
        assertThat(consumerMessages.get(0).stream()).isEqualTo(streamKey);
        assertThat(consumerMessages.get(0).body())
                .containsEntry("event", "created")
                .containsEntry("user", "alice");
        assertThat(acknowledged).isEqualTo(1L);
        assertThat(trimmed).isGreaterThanOrEqualTo(1L);
        assertThat(destroyed).isTrue();
    }

    private String baseUrl(String path) {
        return "http://127.0.0.1:" + port + path;
    }

    private <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static RedisServer buildRedisServer(int port) {
        try {
            return RedisServer.newRedisServer()
                    .port(port)
                    .setting("save \"\"")
                    .setting("appendonly no")
                    .setting("maxmemory 128M")
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create embedded Redis server", e);
        }
    }

    private static int findAvailableTcpPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to find an available TCP port", e);
        }
    }
}
