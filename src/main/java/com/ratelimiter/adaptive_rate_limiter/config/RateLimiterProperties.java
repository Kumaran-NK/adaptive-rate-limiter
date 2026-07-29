package com.ratelimiter.adaptive_rate_limiter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "rate-limiter")
public class RateLimiterProperties {

    private int defaultLimit = 100;
    private int windowSizeSeconds = 60;
    private int redisHealthCheckInterval = 5000;
    private int redisLatencyWarningThreshold = 50;
    private int redisLatencyCriticalThreshold = 200;
    private Cache cache = new Cache();

    public int getDefaultLimit() { return defaultLimit; }
    public void setDefaultLimit(int defaultLimit) { this.defaultLimit = defaultLimit; }

    public int getWindowSizeSeconds() { return windowSizeSeconds; }
    public void setWindowSizeSeconds(int windowSizeSeconds) { this.windowSizeSeconds = windowSizeSeconds; }

    public int getRedisHealthCheckInterval() { return redisHealthCheckInterval; }
    public void setRedisHealthCheckInterval(int redisHealthCheckInterval) { this.redisHealthCheckInterval = redisHealthCheckInterval; }

    public int getRedisLatencyWarningThreshold() { return redisLatencyWarningThreshold; }
    public void setRedisLatencyWarningThreshold(int redisLatencyWarningThreshold) { this.redisLatencyWarningThreshold = redisLatencyWarningThreshold; }

    public int getRedisLatencyCriticalThreshold() { return redisLatencyCriticalThreshold; }
    public void setRedisLatencyCriticalThreshold(int redisLatencyCriticalThreshold) { this.redisLatencyCriticalThreshold = redisLatencyCriticalThreshold; }

    public Cache getCache() { return cache; }
    public void setCache(Cache cache) { this.cache = cache; }

    public static class Cache {
        private int localTtlSeconds = 10;
        private int maxSize = 10000;

        public int getLocalTtlSeconds() { return localTtlSeconds; }
        public void setLocalTtlSeconds(int localTtlSeconds) { this.localTtlSeconds = localTtlSeconds; }

        public int getMaxSize() { return maxSize; }
        public void setMaxSize(int maxSize) { this.maxSize = maxSize; }
    }
}