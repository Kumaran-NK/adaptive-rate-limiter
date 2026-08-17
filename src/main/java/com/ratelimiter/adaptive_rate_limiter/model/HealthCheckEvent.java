package com.ratelimiter.adaptive_rate_limiter.model;

public record HealthCheckEvent(
    double p50LatencyMs,
    double p95LatencyMs,
    double p99LatencyMs,
    double errorRate,
    boolean redisReachable,
    long timestamp,
    // Epoch millis of when ANY instance first observed Redis healthy in the
    // current recovery window, read from a shared Redis key. -1 when Redis
    // is unreachable or the key hasn't been written yet -- callers must fall
    // back to purely local stability tracking in that case.
    long sharedHealthySinceMillis
) {}