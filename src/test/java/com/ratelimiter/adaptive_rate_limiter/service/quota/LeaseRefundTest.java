package com.ratelimiter.adaptive_rate_limiter.service.quota;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * Phase 3C -- real-Redis proof that the refund arm of the batch-GCRA allocator
 * credits back <b>exactly</b> the unused units before granting, in one atomic
 * call. This is what bounds a pod's trapped quota to its lease lifetime instead
 * of the full window: on renewal it returns what it did not spend.
 *
 * <p>Deterministic (no sleeps): the whole scenario runs inside one window, and
 * the emission interval (100ms at limit 100 / window 10s) dwarfs the few ms of
 * real-time drift between calls, so the granted counts are exact.
 */
class LeaseRefundTest extends RedisStrategyTestBase {

    private static final int LIMIT = 100;
    private static final int WINDOW_SECONDS = 10;

    private GcraQuotaAllocator allocator;

    @BeforeEach
    void setUpAllocator() {
        allocator = new GcraQuotaAllocator(redisTemplate, new LuaScriptLoader(),
                new RateLimiterProperties());
    }

    /**
     * Two keys reserve 25 units each. One then re-leases while refunding 22 unused
     * units; the other re-leases refunding nothing. The refunding key gets exactly
     * 22 more units of headroom -- proof the refund returns precisely {@code unused}
     * units of capacity to the shared TAT.
     */
    @Test
    void refundCreditsExactlyUnusedUnitsBackToThePool() {
        String refundKey = uniqueKey("refund");
        String controlKey = uniqueKey("control");

        assertEquals(25, allocator.lease(refundKey, 25, LIMIT, WINDOW_SECONDS).granted(),
                "idle key reserves 25");
        assertEquals(25, allocator.lease(controlKey, 25, LIMIT, WINDOW_SECONDS).granted(),
                "idle key reserves 25");

        // Refund 22 of the 25 held, then over-ask: headroom is (100 - 25) + 22 = 97.
        int withRefund = allocator.lease(refundKey, 5 * LIMIT, 22, LIMIT, WINDOW_SECONDS).granted();
        // No refund: headroom is just (100 - 25) = 75.
        int withoutRefund = allocator.lease(controlKey, 5 * LIMIT, 0, LIMIT, WINDOW_SECONDS).granted();

        assertEquals(withoutRefund + 22, withRefund,
                "refunding 22 unused units must return exactly 22 units of capacity to the pool");
    }

    /**
     * A refund can never push the shared clock before "now": refunding more than
     * was ever reserved on an idle key is clamped, so an idle key still grants at
     * most the full limit -- the refund cannot manufacture extra global capacity.
     */
    @Test
    void refundIsClampedAndNeverExceedsLimitOnAnIdleKey() {
        String key = uniqueKey("refund-clamp");

        int granted = allocator.lease(key, 5 * LIMIT, 5 * LIMIT, LIMIT, WINDOW_SECONDS).granted();

        assertEquals(LIMIT, granted,
                "an over-refund on an idle key is clamped to now; grant stays bounded by limit");
    }
}
