package com.example.redis.example;

public record UserProfile(
        String id,
        String displayName,
        String email,
        String city,
        String status
) {
}
