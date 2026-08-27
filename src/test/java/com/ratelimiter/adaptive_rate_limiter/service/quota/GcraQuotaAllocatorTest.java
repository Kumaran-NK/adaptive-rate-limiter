package com.ratelimiter.adaptive_rate_limiter.service.quota;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * Phase 3A -- real-Redis proof that the batch-GCRA allocator reserves quota by
 * advancing the GCRA TAT (not by decrementing a counter), and that a lease of
 * size 1 reduces EXACTLY to {@code GcraStrategy}'s per-request decision.
 *
 * <p>Uses the same fixture and constants as {@code GcraInvariantTest}
 * (limit 10 / window 10s / emission 1000ms) so the two share one clock model
 * and the TAT arithmetic stays integral.
 */
class GcraQuotaAllocatorTest extends RedisStrategyTestBase {

    private static final int LIMIT = 10;
    private static final int WINDOW_SECONDS = 10;
    private static final long EMISSION_INTERVAL_MS = (WINDOW_SECONDS * 1000L) / LIMIT;

    private GcraQuotaAllocator allocator;

    @BeforeEach
    void setUpAllocator() {
        // Superclass @BeforeEach has already wired redisTemplate + gcraStrategy.
        allocator = new GcraQuotaAllocator(redisTemplate, new LuaScriptLoader(),
                new RateLimiterProperties());
    }

    /**
     * Test 1 -- K=1 equivalence. Driven in lockstep on two independent keys, a
     * lease of size 1 grants exactly when a direct GCRA check admits, across the
     * idle burst and into the saturated/denied region.
     */
    @Test
    void leaseOfOne_isEquivalentToDirectGcra() {
        String gcraKey = uniqueKey("equiv-gcra");
        String allocKey = uniqueKey("equiv-alloc");

        for (int i = 0; i < LIMIT + 3; i++) {
            boolean gcraAllowed = gcraStrategy.isAllowed(gcraKey, LIMIT, WINDOW_SECONDS).allowed();
            int granted = allocator.lease(allocKey, 1, LIMIT, WINDOW_SECONDS).granted();
            assertEquals(gcraAllowed ? 1 : 0, granted,
                    "step " + i + ": lease(K=1) must grant iff direct GCRA admits");
        }
    }

    /**
     * Test 2 -- idle batch. One lease of the whole limit against an idle key
     * grants exactly the limit; the immediate next lease grants nothing.
     */
    @Test
    void idleBatch_grantsExactlyLimit_thenNothing() {
        String key = uniqueKey("idle-batch");

        LeaseGrant first = allocator.lease(key, LIMIT, LIMIT, WINDOW_SECONDS);
        assertEquals(LIMIT, first.granted(), "an idle key grants a full burst of exactly limit");

        LeaseGrant second = allocator.lease(key, LIMIT, LIMIT, WINDOW_SECONDS);
        assertEquals(0, second.granted(), "a saturated key grants nothing on the immediate next lease");
    }

    /**
     * Test 3 -- partial lease. Leasing K &lt; limit grants K and advances the TAT
     * by exactly K emission intervals; a second partial advances it by K more.
     */
    @Test
    void partialLease_advancesTatByGrantedEmissionIntervals() {
        String key = uniqueKey("partial");

        long nowBefore = redisNowMs();
        LeaseGrant first = allocator.lease(key, 4, LIMIT, WINDOW_SECONDS);
        assertEquals(4, first.granted());

        long tat1 = getTatMs(key);
        long expected = nowBefore + 4 * EMISSION_INTERVAL_MS;
        assertTrue(Math.abs(tat1 - expected) <= 200,
                "TAT should sit ~4 emission intervals ahead of now; tat=" + tat1 + " expected~" + expected);

        LeaseGrant second = allocator.lease(key, 4, LIMIT, WINDOW_SECONDS);
        assertEquals(4, second.granted());
        long tat2 = getTatMs(key);
        assertEquals(4 * EMISSION_INTERVAL_MS, tat2 - tat1,
                "a second partial lease advances the TAT by exactly 4 more emission intervals");
    }

    /**
     * Test 4 -- over-ask. Requesting far more than the limit grants at most the
     * limit against an idle key, never more.
     */
    @Test
    void overAsk_neverGrantsMoreThanLimit() {
        String key = uniqueKey("over-ask");

        LeaseGrant grant = allocator.lease(key, 5 * LIMIT, LIMIT, WINDOW_SECONDS);
        assertEquals(LIMIT, grant.granted(), "over-asking an idle key grants exactly limit, never more");

        LeaseGrant again = allocator.lease(key, 5 * LIMIT, LIMIT, WINDOW_SECONDS);
        assertEquals(0, again.granted(), "already saturated: nothing more to grant");
    }

    /**
     * Test 5 -- TAT is monotonic and {@code nextAvailableMs} distinguishes
     * "slack remaining" (0) from "saturated" (&gt; 0).
     */
    @Test
    void tatMonotonic_andNextAvailableSignalsSaturation() {
        String key = uniqueKey("monotonic");

        LeaseGrant g1 = allocator.lease(key, 4, LIMIT, WINDOW_SECONDS); // 4 / 10
        assertEquals(4, g1.granted());
        assertEquals(0, g1.nextAvailableMs(), "slack after 4/10 -> next unit immediately available");
        long tat1 = getTatMs(key);

        LeaseGrant g2 = allocator.lease(key, 4, LIMIT, WINDOW_SECONDS); // 8 / 10
        assertEquals(4, g2.granted());
        assertEquals(0, g2.nextAvailableMs(), "slack after 8/10 -> next unit immediately available");
        long tat2 = getTatMs(key);
        assertTrue(tat2 > tat1, "TAT must advance (monotonic)");

        LeaseGrant g3 = allocator.lease(key, 4, LIMIT, WINDOW_SECONDS); // only 2 remain
        assertEquals(2, g3.granted(), "only the last 2 units of the limit remain");
        assertTrue(g3.nextAvailableMs() > 0, "saturated after 10/10 -> caller must wait for the next unit");
        long tat3 = getTatMs(key);
        assertTrue(tat3 > tat2, "TAT must advance (monotonic)");
    }

    /**
     * Test 6 -- self-expiry. Like {@code gcra.lua}, the key is PEXPIRE'd to its
     * leased virtual time and disappears once that drains.
     */
    @Test
    void leasedKey_selfExpiresAfterVirtualTimeDrains() throws InterruptedException {
        int limit = 5;
        int windowSeconds = 1; // short window -> short TTL, fast test
        String key = uniqueKey("expire");

        LeaseGrant grant = allocator.lease(key, limit, limit, windowSeconds);
        assertEquals(limit, grant.granted());

        String redisKey = redisKey(key);
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)), "key exists right after the lease");

        long ttlMs = redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
        Thread.sleep(Math.max(1L, ttlMs + 150L));

        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)),
                "key must self-expire once its leased virtual time drains");
    }

    // --- helpers (mirror GcraInvariantTest) ---

    private String redisKey(String key) {
        return "ratelimit:" + key + ":gcra";
    }

    private long redisNowMs() {
        Long ms = redisTemplate.execute((RedisCallback<Long>) RedisConnection::time);
        return ms == null ? 0L : ms;
    }

    private long getTatMs(String key) {
        String tatValue = redisTemplate.opsForValue().get(redisKey(key));
        assertNotNull(tatValue, "TAT should exist for key " + key);
        return Math.round(Double.parseDouble(tatValue));
    }
}
