package com.ratelimiter.adaptive_rate_limiter.controller;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.service.RateLimiterService;

@RestController
public class HealthStateController {

    private final RateLimiterService rateLimiterService;

    public HealthStateController(RateLimiterService rateLimiterService) {
        this.rateLimiterService = rateLimiterService;
    }

    @GetMapping("/api/health/state")
    public Map<String, Object> getHealthState() {
        try {
            HealthState state = rateLimiterService.getCurrentHealthState();
            return Map.of(
                    "state", state.name(),
                    "code", state.ordinal(),
                    "timestamp", System.currentTimeMillis()
            );
        } catch (Exception e) {
            return Map.of(
                    "state", "UNKNOWN",
                    "code", -1,
                    "error", e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            );
        }
    }
}