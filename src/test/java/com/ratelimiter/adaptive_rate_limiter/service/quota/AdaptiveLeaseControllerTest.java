package com.ratelimiter.adaptive_rate_limiter.service.quota;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;

/**
 * Phase 3D -- the adaptive lease-size controller in isolation, on a controllable
 * clock so the EWMA + AIMD law is fully deterministic (no sleeps).
 *
 * <p>Proves the four behaviors the design requires: K grows under sustained load,
 * shrinks when idle, always stays within {@code [minLease, maxLeaseFraction x
 * limit]}, and backs off on a partial (contended) grant.
 */
class AdaptiveLeaseControllerTest {

    private static final int LIMIT = 100;
    private static final int MIN_LEASE = 1;
    private static final int MAX_K = 25;         // round(0.25 * 100)
    private static final int TARGET_MS = 2000;

    private AtomicLong clock;
    private AdaptiveLeaseController controller;

    @BeforeEach
    void setUp() {
        clock = new AtomicLong(1_000_000L);
        RateLimiterProperties properties = new RateLimiterProperties();
        RateLimiterProperties.Leasing cfg = properties.getLeasing();
        cfg.setEnabled(true);
        cfg.setAdaptive(true);
        cfg.setMinLease(MIN_LEASE);
        cfg.setMaxLeaseFraction(0.25);           // MAX_K = 25 at limit 100
        cfg.setTargetReleaseIntervalMs(TARGET_MS);
        controller = new AdaptiveLeaseController(properties, clock::get);
    }

    /**
     * Sustained load = leases draining faster than the target interval. K must climb
     * monotonically to the maxLeaseFraction ceiling and stop there.
     */
    @Test
    void growsUnderSustainedLoadUpToMaxK() {
        String key = "hot";
        int prev = 0;
        int last = 0;
        for (int i = 0; i < 10; i++) {
            int size = controller.nextLeaseSize(key, LIMIT);
            assertTrue(size >= prev, "K must not shrink under sustained load (step " + i + ")");
            assertTrue(size <= MAX_K, "K must never exceed maxLeaseFraction x limit");
            prev = size;
            last = size;
            clock.addAndGet(200); // drained in 200ms << 2000ms target -> demand is high
        }
        assertEquals(MAX_K, last, "sustained load must drive K to the ceiling");
    }

    /**
     * Idle = leases draining far slower than target (or sitting until expiry). K must
     * back off multiplicatively down to minLease and floor there.
     */
    @Test
    void shrinksWhenIdleDownToMinLease() {
        String key = "cooling";

        // Ramp to the ceiling first.
        for (int i = 0; i < 8; i++) {
            controller.nextLeaseSize(key, LIMIT);
            clock.addAndGet(200);
        }
        assertEquals(MAX_K, (int) Math.round(controller.currentK(key)), "precondition: at ceiling");

        // Now go idle: each new lease is drawn only after a long gap.
        int prev = MAX_K + 1;
        for (int i = 0; i < 8; i++) {
            clock.addAndGet(10L * TARGET_MS); // 20s gap >> 2s target -> over-provisioned
            int size = controller.nextLeaseSize(key, LIMIT);
            assertTrue(size <= prev, "K must not grow while idle (step " + i + ")");
            assertTrue(size >= MIN_LEASE, "K must never drop below minLease");
            prev = size;
        }
        assertEquals(MIN_LEASE, prev, "sustained idle must drive K down to minLease");
    }

    /**
     * A partial grant means the global budget is contended, so K must back off
     * (multiplicatively). A full grant must leave K untouched.
     */
    @Test
    void partialGrantDrivesKDownFullGrantDoesNot() {
        String key = "contended";

        for (int i = 0; i < 8; i++) {
            controller.nextLeaseSize(key, LIMIT);
            clock.addAndGet(200);
        }
        double atCeiling = controller.currentK(key);
        assertEquals(MAX_K, (int) Math.round(atCeiling), "precondition: at ceiling");

        // Partial grant (asked ceiling, got a few) -> multiplicative backoff.
        controller.recordGrant(key, MAX_K, 3);
        double afterPartial = controller.currentK(key);
        assertTrue(afterPartial < atCeiling, "a partial grant must drive K down");
        assertEquals(atCeiling * 0.5, afterPartial, 1e-9, "backoff is multiplicative (x0.5)");

        // A full grant is not a contention signal -> K unchanged.
        int fullReq = (int) Math.round(afterPartial);
        controller.recordGrant(key, fullReq, fullReq);
        assertEquals(afterPartial, controller.currentK(key), 1e-9, "a full grant must not shrink K");
    }

    /**
     * Across an arbitrary mix of fast load, idle gaps, and a contended grant, every
     * size handed out -- and the internal estimate -- stays within the bounds.
     */
    @Test
    void neverLeavesBoundsAcrossMixedLoad() {
        String key = "mixed";
        long[] gaps = {200, 200, 200, 50, 30_000, 200, 15_000, 200, 200, 40_000};
        for (int i = 0; i < gaps.length; i++) {
            int size = controller.nextLeaseSize(key, LIMIT);
            assertTrue(size >= MIN_LEASE && size <= MAX_K,
                    "size " + size + " out of [" + MIN_LEASE + "," + MAX_K + "] at step " + i);
            assertTrue(controller.currentK(key) >= MIN_LEASE && controller.currentK(key) <= MAX_K,
                    "internal K out of bounds at step " + i);
            if (i == 3) {
                controller.recordGrant(key, size, 1); // inject a contended grant mid-stream
                assertTrue(controller.currentK(key) >= MIN_LEASE, "backoff must not underflow minLease");
            }
            clock.addAndGet(gaps[i]);
        }
    }
}
