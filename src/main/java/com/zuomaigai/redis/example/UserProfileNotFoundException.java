package com.zuomaigai.redis.example;

public class UserProfileNotFoundException extends RuntimeException {

    public UserProfileNotFoundException(String id) {
        super("User profile not found: " + id);
    }
}
