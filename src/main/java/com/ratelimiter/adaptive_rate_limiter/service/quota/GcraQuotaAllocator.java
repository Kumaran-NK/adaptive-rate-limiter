package com.ratelimiter.adaptive_rate_limiter.service.quota;

import java.util.List;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;

/**
 * Batch-GCRA quota allocator (Phase 3A).
 *
 * <p>Reserves quota by advancing the SAME GCRA Theoretical Arrival Time (TAT)
 * that {@link com.ratelimiter.adaptive_rate_limiter.service.strategy.GcraStrategy}
 * uses -- by {@code granted * emission_interval} in one atomic step -- rather
 * than decrementing a plain counter. Advancing the shared TAT IS the distributed
 * coordination mechanism, so a lease of size 1 reduces EXACTLY to GcraStrategy's
 * per-request decision (see {@code GcraQuotaAllocatorTest}). Deliberately mirrors
 * {@code GcraStrategy}'s construction and fail-safe conventions, and operates on
 * the identical key ({@code ratelimit:<key>:gcra}) -- one TAT per key.
 *
 * <p>Runs {@code lua/lease_quota.lua}. The {@code unused} argument credits a
 * caller's previously-held-but-unspent units back to the shared TAT before
 * granting, in the same atomic call (Phase 3C refund).
 */
@Component
public class GcraQuotaAllocator implements QuotaAllocator {

    private final RedisTemplate<String, String> redisTemplate;

    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> leaseScript;

    private final RateLimiterProperties properties;

    public GcraQuotaAllocator(RedisTemplate<String, String> redisTemplate,
                              LuaScriptLoader scriptLoader,
                              RateLimiterProperties properties) {
        this.redisTemplate = redisTemplate;
        this.leaseScript = scriptLoader.getLeaseQuotaScript();
        this.properties = properties;
    }

    @Override
    public LeaseGrant lease(String key, int requested, int unused, int limit, int windowSeconds) {
        String redisKey = "ratelimit:" + key + ":gcra";
        long periodMs = windowSeconds * 1000L;

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(
                leaseScript,
                List.of(redisKey),
                String.valueOf(periodMs),
                String.valueOf(limit),
                String.valueOf(requested),
                String.valueOf(Math.max(0, unused))  // refund unused units from the prior lease
        );

        if (result == null || result.size() < 4) {
            // Fail-safe: grant nothing rather than admit against an unverified
            // decision (deny-by-default, exactly like GcraStrategy).
            return new LeaseGrant(0, System.currentTimeMillis(), 0L);
        }

        int granted = result.get(0) != null ? result.get(0).intValue() : 0;
        // result.get(1) is the new TAT -- observability only; not surfaced.
        long redisNowMs = result.get(2) != null ? result.get(2) : System.currentTimeMillis();
        long nextAvailableMs = result.get(3) != null ? result.get(3) : 0L;

        long leaseExpiryMs = redisNowMs + properties.getLeasing().getLeaseTtlMs();
        return new LeaseGrant(granted, leaseExpiryMs, nextAvailableMs);
    }
}
