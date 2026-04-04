package com.example.redis.example;

public record UserProfileUpsertRequest(
        String displayName,
        String email,
        String city
) {
}
