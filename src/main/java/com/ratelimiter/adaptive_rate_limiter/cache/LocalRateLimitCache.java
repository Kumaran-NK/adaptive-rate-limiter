package com.ratelimiter.adaptive_rate_limiter.cache;

import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.stereotype.Component;

@Component
public class LocalRateLimitCache {

    private final Cache<String, Integer> cache;

    public LocalRateLimitCache(Cache<String, Integer> rateLimitCache) {
        this.cache = rateLimitCache;
    }

    public void put(String key, int count) {
        cache.put(key, count);
    }

    public Integer get(String key) {
        return cache.getIfPresent(key);
    }

    public void invalidate(String key) {
        cache.invalidate(key);
    }

    public double hitRate() {
        return cache.stats().hitRate();
    }
}