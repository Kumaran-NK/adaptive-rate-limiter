package com.ratelimiter.adaptive_rate_limiter.model;

import java.time.Instant;

public record StateTransition(
    HealthState from,
    HealthState to,
    String reason,
    Instant timestamp
) {}