package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;

public interface RateLimitStrategy {
    RateLimitDecision isAllowed(String key, int limit, int windowSeconds);
    String getAlgorithmName();
}
