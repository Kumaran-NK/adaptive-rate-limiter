package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * Runs identical request patterns against Sliding Window and GCRA and
 * compares their decisions. The two algorithms do NOT have identical
 * semantics, so this suite does not assert they always agree -- it asserts
 * where they agree (instantaneous burst capacity), and demonstrates and
 * documents where and why they legitimately diverge (post-burst recovery
 * pacing), per the experiment's goals.
 */
class AlgorithmComparisonTest extends RedisStrategyTestBase {

    @Test
    void bothAlgorithmsAdmitExactlyLimitRequestsOnAnInstantaneousBurstAgainstAnIdleKey() {
        // Both algorithms are deliberately parameterized (Sliding Window
        // inherently, GCRA via DVT = period_ms) so that a burst against a
        // fully idle key admits exactly `limit` requests and rejects the
        // (limit+1)-th. This is the one point where their decisions must
        // agree for every request in the pattern.
        int limit = 15;
        int windowSeconds = 10;
        String swKey = uniqueKey("cmp-sw-burst");
        String gcraKey = uniqueKey("cmp-gcra-burst");

        for (int i = 0; i < limit; i++) {
            RateLimitDecision sw = slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds);
            RateLimitDecision gcra = gcraStrategy.isAllowed(gcraKey, limit, windowSeconds);
            assertEquals(sw.allowed(), gcra.allowed(), "request " + i + " should agree between algorithms");
            assertTrue(sw.allowed());
        }

        RateLimitDecision swOver = slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds);
        RateLimitDecision gcraOver = gcraStrategy.isAllowed(gcraKey, limit, windowSeconds);
        assertFalse(swOver.allowed());
        assertFalse(gcraOver.allowed());
    }

    @Test
    void gcraRecoversCapacityFasterThanSlidingWindowAfterAPartialBurst() throws InterruptedException {
        // This is the key, expected DIVERGENCE between the two algorithms:
        //
        //  - Sliding Window is an exact historical counter. Each of the
        //    `limit` timestamps individually ages out of the sorted set
        //    exactly `window` seconds after it was recorded. A slot only
        //    frees up when that specific entry crosses the window boundary.
        //
        //  - GCRA is a pacing/meter algorithm. Once a burst has been used,
        //    GCRA resumes admitting new requests at the steady rate
        //    (limit / window) almost immediately -- as soon as one
        //    emission_interval elapses -- because its TAT is clamped to
        //    "now" whenever it has fallen behind, effectively forgetting
        //    burst history once the queue has caught up.
        //
        // Neither behavior is a bug: Sliding Window guarantees an exact
        // rolling count; GCRA guarantees smooth, continuously-recovering
        // throughput. For a rate limiter protecting a downstream dependency,
        // GCRA's faster, smoother recovery is often *more* desirable (it
        // avoids the "all N slots frozen for a full window" cliff), but it
        // means GCRA is less strict about enforcing "no more than N in any
        // W-second span" than Sliding Window is.
        int limit = 10;
        int windowSeconds = 1;
        long emissionIntervalMs = (windowSeconds * 1000L) / limit; // 100ms
        String swKey = uniqueKey("cmp-sw-recovery");
        String gcraKey = uniqueKey("cmp-gcra-recovery");

        // Exhaust the full burst on both, back-to-back.
        for (int i = 0; i < limit; i++) {
            assertTrue(slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds).allowed());
            assertTrue(gcraStrategy.isAllowed(gcraKey, limit, windowSeconds).allowed());
        }
        assertFalse(slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds).allowed());
        assertFalse(gcraStrategy.isAllowed(gcraKey, limit, windowSeconds).allowed());

        // Wait long enough for GCRA to free exactly one slot (a bit more
        // than one emission_interval), but nowhere near a full window.
        Thread.sleep(emissionIntervalMs * 4);

        RateLimitDecision gcraAfterShortWait = gcraStrategy.isAllowed(gcraKey, limit, windowSeconds);
        RateLimitDecision swAfterShortWait = slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds);

        assertTrue(gcraAfterShortWait.allowed(),
                "GCRA should already have recovered at least one slot after a few emission intervals");
        assertFalse(swAfterShortWait.allowed(),
                "Sliding Window must still deny -- none of the original entries have aged past the full window yet");
    }

    @Test
    void bothAlgorithmsEnforceTheirLimitAcrossAWindowBoundary() throws InterruptedException {
        // Classic "boundary" scrutiny: verify that requesting right around
        // a window boundary does not let either algorithm exceed its own
        // guarantee within any trailing window. Note the existing
        // SlidingWindowStrategy is a TRUE sliding window (a Redis sorted
        // set of real timestamps, continuously trimmed via
        // ZREMRANGEBYSCORE), not a naive fixed/bucketed window, so it does
        // NOT suffer the classic fixed-window "double burst at the
        // boundary" flaw (e.g. N at 00:59.9 + N at 01:00.1 = 2N in 200ms).
        int limit = 10;
        int windowSeconds = 10;
        String swKey = uniqueKey("cmp-sw-boundary");
        String gcraKey = uniqueKey("cmp-gcra-boundary");

        // Burst right at t=0.
        int swAllowedFirstBurst = 0;
        int gcraAllowedFirstBurst = 0;
        for (int i = 0; i < limit; i++) {
            if (slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds).allowed()) swAllowedFirstBurst++;
            if (gcraStrategy.isAllowed(gcraKey, limit, windowSeconds).allowed()) gcraAllowedFirstBurst++;
        }
        assertEquals(limit, swAllowedFirstBurst);
        assertEquals(limit, gcraAllowedFirstBurst);

        // Immediately after (well inside the same window): both must deny.
        assertFalse(slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds).allowed());

        // Sleep just past the window boundary.
        Thread.sleep((windowSeconds * 1000L) + 50);

        // Sliding Window: the original burst has now fully aged out, so a
        // fresh full burst of `limit` should be available again -- never
        // more than `limit`.
        int swAllowedSecondBurst = 0;
        for (int i = 0; i < limit + 2; i++) {
            if (slidingWindowStrategy.isAllowed(swKey, limit, windowSeconds).allowed()) swAllowedSecondBurst++;
        }
        assertEquals(limit, swAllowedSecondBurst,
                "Sliding Window must allow exactly a fresh `limit` burst once the prior window has fully elapsed, "
                        + "never more");

        // GCRA: by this point roughly a full window (10 emission
        // intervals) has elapsed since the last admission, so its TAT has
        // long since decayed back to idle -- a fresh full burst is
        // available too, and still never more than `limit`.
        int gcraAllowedSecondBurst = 0;
        for (int i = 0; i < limit + 2; i++) {
            if (gcraStrategy.isAllowed(gcraKey, limit, windowSeconds).allowed()) gcraAllowedSecondBurst++;
        }
        assertEquals(limit, gcraAllowedSecondBurst,
                "GCRA must also allow exactly a fresh `limit` burst once its TAT has fully decayed, never more");
    }
}