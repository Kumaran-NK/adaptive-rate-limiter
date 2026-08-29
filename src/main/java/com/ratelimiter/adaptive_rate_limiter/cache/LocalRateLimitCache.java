package com.ratelimiter.adaptive_rate_limiter.cache;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;

@Component
public class LocalRateLimitCache {

    public record ConsumeResult(boolean allowed, int remaining) {}

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

    public ConsumeResult tryConsume(String key) {
        AtomicReference<ConsumeResult> result = new AtomicReference<>(new ConsumeResult(false, 0));
        cache.asMap().computeIfPresent(key, (k, current) -> {
            if (current != null && current > 0) {
                int remaining = current - 1;
                result.set(new ConsumeResult(true, remaining));
                return remaining;
            }
            result.set(new ConsumeResult(false, 0));
            return current;
        });
        return result.get();
    }

    public void invalidate(String key) {
        cache.invalidate(key);
    }

    public double hitRate() {
        return cache.stats().hitRate();
    }
}