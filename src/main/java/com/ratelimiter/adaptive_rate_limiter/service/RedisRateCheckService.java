package com.ratelimiter.adaptive_rate_limiter.service;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.TokenBucketStrategy;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

@Service
public class RedisRateCheckService {

    private static final Logger log = LoggerFactory.getLogger(RedisRateCheckService.class);

    private final SlidingWindowStrategy slidingWindowStrategy;
    private final TokenBucketStrategy tokenBucketStrategy;
    private final CircuitBreaker circuitBreaker;

    public RedisRateCheckService(SlidingWindowStrategy slidingWindowStrategy,
                                  TokenBucketStrategy tokenBucketStrategy,
                                  CircuitBreaker circuitBreaker) {
        this.slidingWindowStrategy = slidingWindowStrategy;
        this.tokenBucketStrategy = tokenBucketStrategy;
        this.circuitBreaker = circuitBreaker;
    }

    public RateLimitDecision checkWithRedis(String key, int limit, int windowSeconds, String endpoint) {
        Supplier<RateLimitDecision> redisCall = () ->
                slidingWindowStrategy.isAllowed(key, limit, windowSeconds);

        try {
            return circuitBreaker.executeSupplier(redisCall);
        } catch (Exception e) {
            log.debug("Circuit breaker fallback: {} - {}", endpoint, e.getMessage());
            return tokenBucketStrategy.isAllowed(key, limit, windowSeconds);
        }
    }
}