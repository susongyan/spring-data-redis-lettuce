package com.example.redis.example;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
public class UserProfileController {

    private final UserProfileService userProfileService;

    public UserProfileController(UserProfileService userProfileService) {
        this.userProfileService = userProfileService;
    }

    @PutMapping("/{id}")
    public CompletionStage<UserProfile> putProfile(
            @PathVariable String id,
            @RequestBody UserProfileUpsertRequest request
    ) {
        return userProfileService.save(id, request);
    }

    @PutMapping("/blocking/{id}")
    public ResponseEntity<UserProfile> putProfileBlocking(
            @PathVariable String id,
            @RequestBody UserProfileUpsertRequest request
    ) {
        return ResponseEntity.ok(userProfileService.saveBlocking(id, request));
    }

    @GetMapping("/{id}")
    public CompletionStage<ResponseEntity<UserProfile>> getProfile(@PathVariable String id) {
        return userProfileService.get(id)
                .thenApply(profile -> profile == null
                        ? ResponseEntity.notFound().build()
                        : ResponseEntity.ok(profile));
    }

    @GetMapping("/blocking/{id}")
    public ResponseEntity<UserProfile> getProfileBlocking(@PathVariable String id) {
        UserProfile profile = userProfileService.getBlocking(id);
        if (profile == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(profile);
    }

    @PutMapping("/{id}/status")
    public CompletionStage<ResponseEntity<Void>> updateStatus(
            @PathVariable String id,
            @RequestParam String value
    ) {
        return userProfileService.updateStatus(id, value)
                .thenApply(ignored -> ResponseEntity.noContent().build());
    }

    @DeleteMapping("/{id}")
    public CompletionStage<ResponseEntity<Void>> deleteProfile(@PathVariable String id) {
        return userProfileService.delete(id)
                .thenApply(deleted -> deleted
                        ? ResponseEntity.noContent().build()
                        : ResponseEntity.notFound().build());
    }

    @ExceptionHandler(UserProfileNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleNotFound(UserProfileNotFoundException exception) {
        return ResponseEntity.notFound().build();
    }
}
