package com.zuomaigai.redis.example;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.zuomaigai.redis.async.api.AsyncRedisTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class UserProfileService {

    private static final Duration STATUS_TTL = Duration.ofMinutes(30);
    private static final long BLOCKING_DEMO_TIMEOUT_SECONDS = 5L;

    private final AsyncRedisTemplate<String, String> redisTemplate;

    public UserProfileService(
            @Qualifier("asyncStringRedisTemplate") AsyncRedisTemplate<String, String> redisTemplate
    ) {
        this.redisTemplate = redisTemplate;
    }

    public CompletionStage<UserProfile> save(String id, UserProfileUpsertRequest request) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("displayName", request.displayName());
        fields.put("email", request.email());
        fields.put("city", request.city());

        return redisTemplate.<String, String>opsForHash()
                .putAll(profileKey(id), fields)
                .thenCompose(ignored -> redisTemplate.opsForValue().set(statusKey(id), "ACTIVE", STATUS_TTL))
                .thenCompose(ignored -> get(id));
    }

    public UserProfile saveBlocking(String id, UserProfileUpsertRequest request) {
        return await(save(id, request));
    }

    public CompletionStage<UserProfile> get(String id) {
        CompletionStage<Map<String, String>> profileStage =
                redisTemplate.<String, String>opsForHash().entries(profileKey(id));
        CompletionStage<String> statusStage = redisTemplate.opsForValue().get(statusKey(id));

        return profileStage.thenCombine(statusStage, (entries, status) -> {
            if (entries == null || entries.isEmpty()) {
                return null;
            }
            return new UserProfile(
                    id,
                    entries.get("displayName"),
                    entries.get("email"),
                    entries.get("city"),
                    status
            );
        });
    }

    public UserProfile getBlocking(String id) {
        return await(get(id));
    }

    public CompletionStage<Boolean> delete(String id) {
        return redisTemplate.delete(java.util.List.of(profileKey(id), statusKey(id)))
                .thenApply(deleted -> deleted != null && deleted > 0);
    }

    public CompletionStage<Void> updateStatus(String id, String status) {
        return ensureProfileExists(id).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.failedStage(new UserProfileNotFoundException(id));
            }
            return redisTemplate.opsForValue().set(statusKey(id), status, STATUS_TTL);
        });
    }

    private CompletionStage<Boolean> ensureProfileExists(String id) {
        return redisTemplate.hasKey(profileKey(id));
    }

    // Demo only: bridge async Redis access back into a synchronous caller with a bounded wait.
    private <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(BLOCKING_DEMO_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for async Redis result", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timed out while waiting for async Redis result", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Async Redis call failed", e.getCause());
        }
    }

    private String profileKey(String id) {
        return "example:user:profile:" + id;
    }

    private String statusKey(String id) {
        return "example:user:status:" + id;
    }
}
