package com.example.redis.example;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.example.redis.async.api.AsyncRedisTemplate;
import com.example.redis.async.connection.AsyncRedisConnectionProvider;
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

    private String baseUrl(String path) {
        return "http://127.0.0.1:" + port + path;
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
