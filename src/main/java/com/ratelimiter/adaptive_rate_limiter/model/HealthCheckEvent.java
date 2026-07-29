package com.ratelimiter.adaptive_rate_limiter.model;

public record HealthCheckEvent(
    double p50LatencyMs,
    double p95LatencyMs,
    double p99LatencyMs,
    double errorRate,
    boolean redisReachable,
    long timestamp
) {}