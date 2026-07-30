package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;

@Component
public class TokenBucketStrategy implements RateLimitStrategy {

    private final Cache<String, TokenBucket> buckets;

    public TokenBucketStrategy() {
        this.buckets = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .maximumSize(10000)
                .build();
    }

    @Override
    public RateLimitDecision isAllowed(String key, int limit, int windowSeconds) {
        TokenBucket bucket = buckets.get(key, k -> new TokenBucket(limit, windowSeconds));

        synchronized (bucket) {
            bucket.refill();
            if (bucket.tokens > 0) {
                bucket.tokens--;
                return RateLimitDecision.allowed(
                        (int) bucket.tokens,
                        bucket.nextRefillTime(),
                        HealthState.DEGRADED,
                        "TOKEN_BUCKET"
                );
            }
            return RateLimitDecision.denied(
                    bucket.nextRefillTime(),
                    HealthState.DEGRADED,
                    "TOKEN_BUCKET"
            );
        }
    }

    @Override
    public String getAlgorithmName() {
        return "TOKEN_BUCKET";
    }

    private static class TokenBucket {
        double tokens;
        long lastRefill;
        final int capacity;
        final double refillRate;

        TokenBucket(int capacity, int windowSeconds) {
            this.capacity = capacity;
            this.tokens = capacity;
            this.lastRefill = System.currentTimeMillis();
            this.refillRate = (double) capacity / (windowSeconds * 1000.0);
        }

        void refill() {
            long now = System.currentTimeMillis();
            double elapsed = now - lastRefill;
            double newTokens = elapsed * refillRate;
            tokens = Math.min(capacity, tokens + newTokens);
            lastRefill = now;
        }

        long nextRefillTime() {
            if (tokens >= 1.0) return System.currentTimeMillis();
            double needed = 1.0 - tokens;
            return System.currentTimeMillis() + (long) (needed / refillRate);
        }
    }
}