package com.ratelimiter.adaptive_rate_limiter.model;

public record RateLimitDecision(
    boolean allowed,
    int remaining,
    long resetTimeMillis,
    HealthState systemHealth,
    String algorithmUsed
) {
    public static RateLimitDecision allowed(int remaining, long resetTimeMillis,
                                             HealthState health, String algorithm) {
        return new RateLimitDecision(true, remaining, resetTimeMillis, health, algorithm);
    }

    public static RateLimitDecision denied(long resetTimeMillis,
                                            HealthState health, String algorithm) {
        return new RateLimitDecision(false, 0, resetTimeMillis, health, algorithm);
    }
}