package com.ratelimiter.adaptive_rate_limiter.config;

import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.Caffeine;

@Configuration
public class CacheConfig {

    private final RateLimiterProperties properties;

    public CacheConfig(RateLimiterProperties properties) {
        this.properties = properties;
    }

    @Bean
    public com.github.benmanes.caffeine.cache.Cache<String, Integer> rateLimitCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(properties.getCache().getLocalTtlSeconds(), TimeUnit.SECONDS)
                .maximumSize(properties.getCache().getMaxSize())
                .recordStats()
                .build();
    }
}