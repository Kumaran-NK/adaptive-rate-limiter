package com.ratelimiter.adaptive_rate_limiter.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@RestController
public class RateLimitTestController {

    @GetMapping("/api/test")
    public Map<String, Object> testEndpoint(@RequestParam(defaultValue = "test-user") String key) {
        return Map.of(
                "message", "Request successful",
                "key", key,
                "timestamp", Instant.now().toString()
        );
    }

    @GetMapping("/api/payment")
    public Map<String, Object> paymentEndpoint() {
        return Map.of(
                "message", "Payment processed",
                "timestamp", Instant.now().toString()
        );
    }

    @GetMapping("/api/search")
    public Map<String, Object> searchEndpoint() {
        return Map.of(
                "message", "Search results",
                "timestamp", Instant.now().toString()
        );
    }

    @GetMapping("/api/ai-inference")
    public Map<String, Object> aiInferenceEndpoint() {
        return Map.of(
                "message", "AI inference complete",
                "timestamp", Instant.now().toString()
        );
    }
}