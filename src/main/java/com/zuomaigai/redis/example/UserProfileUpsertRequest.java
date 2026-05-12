package com.zuomaigai.redis.example;

public record UserProfileUpsertRequest(
        String displayName,
        String email,
        String city
) {
}
