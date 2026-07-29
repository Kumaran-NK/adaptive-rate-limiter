package com.ratelimiter.adaptive_rate_limiter.model;

public enum HealthState {
    HEALTHY,
    WARNING,
    DEGRADED,
    RECOVERY
}