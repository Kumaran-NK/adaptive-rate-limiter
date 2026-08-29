package com.ratelimiter.adaptive_rate_limiter.service.quota;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;

class LeaseGrantClockTest {

    @Test
    void leaseExpiryShouldBeBasedOnLocalPodClockNotRedisClock() {
        @SuppressWarnings("unchecked")
        RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
        LuaScriptLoader scriptLoader = mock(LuaScriptLoader.class);
        when(scriptLoader.getLeaseQuotaScript()).thenReturn(mock(DefaultRedisScript.class));

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.getLeasing().setLeaseTtlMs(5000);

        // Redis clock is simulated 1 hour in the past (clock skew)
        long simulatedRedisNowMs = System.currentTimeMillis() - 3_600_000L;
        List<Long> mockScriptResult = List.of(5L, 0L, simulatedRedisNowMs, 0L);

        when(redisTemplate.execute(any(DefaultRedisScript.class), any(List.class), any(Object[].class)))
                .thenReturn(mockScriptResult);

        GcraQuotaAllocator allocator = new GcraQuotaAllocator(redisTemplate, scriptLoader, properties);

        long localBefore = System.currentTimeMillis();
        LeaseGrant grant = allocator.lease("test-key", 5, 0, 100, 60);
        long localAfter = System.currentTimeMillis();

        long expectedExpiryMin = localBefore + 5000;
        long expectedExpiryMax = localAfter + 5000;

        assertTrue(grant.leaseExpiryMs() >= expectedExpiryMin && grant.leaseExpiryMs() <= expectedExpiryMax,
                "Lease expiry (" + grant.leaseExpiryMs() + ") must be calculated relative to local pod clock, not Redis clock (" + simulatedRedisNowMs + ")");
    }
}
