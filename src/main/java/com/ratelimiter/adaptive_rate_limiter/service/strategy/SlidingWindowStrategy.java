package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;

@Component
public class SlidingWindowStrategy implements RateLimitStrategy {

    private final RedisTemplate<String, String> redisTemplate;
    private final DefaultRedisScript<List> slidingWindowScript;

    public SlidingWindowStrategy(RedisTemplate<String, String> redisTemplate,
                                  LuaScriptLoader scriptLoader) {
        this.redisTemplate = redisTemplate;
        this.slidingWindowScript = scriptLoader.getSlidingWindowScript();
    }

    @Override
    public RateLimitDecision isAllowed(String key, int limit, int windowSeconds) {
        String redisKey = "ratelimit:" + key + ":window";
        long now = System.currentTimeMillis();
        long windowMs = windowSeconds * 1000L;
        String requestId = UUID.randomUUID().toString();

        List<Long> result = redisTemplate.execute(
                slidingWindowScript,
                List.of(redisKey),
                String.valueOf(now),
                String.valueOf(windowMs),
                String.valueOf(limit),
                requestId
        );

        if (result == null || result.isEmpty()) {
            return RateLimitDecision.denied(now + windowMs, HealthState.HEALTHY, "SLIDING_WINDOW");
        }

        boolean allowed = result.get(0) == 1;
        int remaining = result.get(1) != null ? result.get(1).intValue() : 0;
        long resetTime = result.get(2) != null ? result.get(2) : now + windowMs;

        if (allowed) {
            return RateLimitDecision.allowed(remaining, resetTime, HealthState.HEALTHY, "SLIDING_WINDOW");
        } else {
            return RateLimitDecision.denied(resetTime, HealthState.HEALTHY, "SLIDING_WINDOW");
        }
    }

    @Override
    public String getAlgorithmName() {
        return "SLIDING_WINDOW";
    }
}