package com.ratelimiter.adaptive_rate_limiter.service.quota;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.RateLimiterMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Phase 3B -- single-pod proof that {@code LeaseManager} admits locally against a
 * leased batch, paying Redis only once per K admits, and that it prefetches ahead
 * of exhaustion.
 *
 * <p>Uses a fake in-memory {@link QuotaAllocator} rather than Testcontainers: the
 * allocator&harr;Redis batch-GCRA path is already proven by
 * {@code GcraQuotaAllocatorTest}, so here we isolate the local bucket + lease
 * bookkeeping. A default (CLOSED) {@code "redis"} circuit breaker executes the
 * lease supplier inline. Config is tuned for a clean K: {@code limit = 100} with
 * {@code maxLeaseFraction = 0.1} gives {@code K = 10}.
 */
class LeaseManagerTest {

    private static final int LIMIT = 100;
    private static final int WINDOW_SECONDS = 60;
    private static final int K = 10; // round(0.1 * 100)

    private FakeAllocator allocator;
    private RateLimiterProperties properties;
    private AdaptiveLeaseController controller;

    @BeforeEach
    void setUp() {
        allocator = new FakeAllocator();
        properties = new RateLimiterProperties();
        properties.setWindowSizeSeconds(WINDOW_SECONDS);
        RateLimiterProperties.Leasing leasing = properties.getLeasing();
        leasing.setEnabled(true);
        leasing.setMinLease(1);
        leasing.setMaxLeaseFraction(0.1);   // K = 10 at limit 100
        leasing.setPrefetchWatermark(0.0);  // prefetch OFF by default; tests opt in
        controller = new AdaptiveLeaseController(properties);
    }

    private LeaseManager newManager() {
        return new LeaseManager(allocator, properties, CircuitBreakerRegistry.ofDefaults(), controller,
                new RateLimiterMetrics(new SimpleMeterRegistry()));
    }

    /**
     * Test 1 -- one lease serves K admits. The first admit draws a lease; the next
     * K-1 are served purely from the local bucket with no further Redis calls.
     */
    @Test
    void servesKAdmitsFromASingleLease() {
        LeaseManager manager = newManager();
        String key = "local-batch";

        for (int i = 0; i < K; i++) {
            RateLimitDecision d = manager.tryAdmit(key, LIMIT, WINDOW_SECONDS);
            assertNotNull(d, "lease should succeed");
            assertTrue(d.allowed(), "admit " + i + " must be allowed while the lease has tokens");
        }

        assertEquals(1, allocator.leaseCalls.get(), "K admits must cost exactly one lease (one Redis call)");
        assertEquals(1, manager.syncLeaseCount(), "exactly one synchronous (empty-bucket) lease");
        assertEquals(K - 1, manager.localAdmitCount(), "the other K-1 admits are served locally, no Redis");
    }

    /**
     * Test 2 -- Redis calls == admits / K, exactly. With prefetch disabled, each
     * batch is fully drained before the next lease, so N admits cost N/K leases.
     */
    @Test
    void redisCallsEqualAdmitsDividedByK() {
        LeaseManager manager = newManager();
        String key = "batched";
        int admits = 100; // 10 full batches of K=10

        for (int i = 0; i < admits; i++) {
            assertTrue(manager.tryAdmit(key, LIMIT, WINDOW_SECONDS).allowed());
        }

        assertEquals(admits / K, allocator.leaseCalls.get(),
                "leasing collapses " + admits + " admits into " + (admits / K) + " Redis calls");
    }

    /**
     * Test 3 -- prefetch fires at the watermark and refills asynchronously, so the
     * refill overlaps serving instead of blocking on an empty bucket. With
     * watermark 0.2 and K=10, the re-lease triggers once remaining drops to 2.
     */
    @Test
    void prefetchesAtWatermarkAheadOfExhaustion() {
        properties.getLeasing().setPrefetchWatermark(0.2); // re-lease at remaining <= 2
        LeaseManager manager = newManager();
        String key = "prefetch";

        // Drain the first batch down to the watermark (8 admits: remaining 10 -> 2).
        for (int i = 0; i < 8; i++) {
            assertTrue(manager.tryAdmit(key, LIMIT, WINDOW_SECONDS).allowed());
        }

        // The prefetch is async; wait briefly for it to complete.
        awaitCondition(() -> manager.prefetchLeaseCount() >= 1, 1000);

        assertTrue(manager.prefetchLeaseCount() >= 1, "a prefetch lease must fire at the watermark");
        assertEquals(1, manager.syncLeaseCount(),
                "only the initial empty-bucket lease is synchronous; the refill is a prefetch");
    }

    /**
     * Test 4 -- degrade signal. When the allocator cannot serve (Redis down) and
     * the bucket is empty, tryAdmit returns {@code null} so the caller degrades --
     * it never propagates a raw Redis error onto the request path.
     */
    @Test
    void returnsNullToSignalDegradeWhenLeaseUnavailableAndBucketEmpty() {
        allocator.fail = true;
        LeaseManager manager = newManager();

        RateLimitDecision d = manager.tryAdmit("cold", LIMIT, WINDOW_SECONDS);

        assertNull(d, "empty bucket + unavailable Redis must signal degrade (null), not throw");
    }

    /**
     * Test 5 (Phase 3C) -- survive then degrade. A held lease keeps serving after
     * Redis dies (the fast path touches no Redis), and only once it drains does the
     * manager signal degrade. No admit throws while a valid lease is held.
     */
    @Test
    void heldLeaseKeepsServingAfterRedisDies_thenSignalsDegradeOnExhaustion() {
        LeaseManager manager = newManager(); // K=10, prefetch off
        String key = "survive";

        assertTrue(manager.tryAdmit(key, LIMIT, WINDOW_SECONDS).allowed(), "first admit draws a lease of K");
        assertEquals(1, allocator.leaseCalls.get());

        allocator.fail = true; // Redis goes down mid-lease

        for (int i = 0; i < K - 1; i++) {
            RateLimitDecision d = manager.tryAdmit(key, LIMIT, WINDOW_SECONDS);
            assertNotNull(d, "held tokens must keep serving while Redis is down");
            assertTrue(d.allowed(), "held token " + i + " must be admitted from the local lease");
        }
        assertEquals(1, allocator.leaseCalls.get(), "the K-1 held-token admits must touch no Redis");

        assertNull(manager.tryAdmit(key, LIMIT, WINDOW_SECONDS),
                "drained lease + Redis still down -> degrade signal (null)");
    }

    /**
     * Test 6 (Phase 3C) -- RECOVERY leases conservatively. In conservative mode the
     * manager leases only minLease units, not the full K, so a just-recovered Redis
     * is eased back into rather than hit with full-size batches.
     */
    @Test
    void recoveryModeLeasesAtMinLeaseNotFullK() {
        properties.getLeasing().setMinLease(1);
        LeaseManager manager = newManager();

        manager.tryAdmit("recovering", LIMIT, WINDOW_SECONDS, true);
        assertEquals(1, allocator.lastRequested, "RECOVERY must lease at minLease (1)");

        manager.tryAdmit("healthy", LIMIT, WINDOW_SECONDS);
        assertEquals(K, allocator.lastRequested, "HEALTHY leases the full K=" + K);
    }

    /**
     * Test 7 (Phase 3D) -- when {@code leasing.adaptive} is on, the manager sizes
     * leases via the {@link AdaptiveLeaseController} instead of the fixed K. A cold
     * key starts its AIMD ramp at {@code minLease}, so the first lease is minLease,
     * not the fixed K -- proof the adaptive path is genuinely wired in and bypasses
     * {@code leaseSizeFor}.
     */
    @Test
    void adaptiveModeSizesLeaseViaControllerNotFixedK() {
        properties.getLeasing().setAdaptive(true);
        properties.getLeasing().setMinLease(2);
        LeaseManager manager = newManager();

        manager.tryAdmit("adaptive-cold", LIMIT, WINDOW_SECONDS);

        assertEquals(2, allocator.lastRequested,
                "adaptive cold-start leases at minLease (2), not the fixed K=" + K);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * In-memory allocator: grants exactly what is requested (a generous global
     * budget) and records call count / last args. {@code fail} simulates Redis
     * being unreachable (the allocator throws, as a real Redis outage would).
     */
    private static final class FakeAllocator implements QuotaAllocator {
        final AtomicInteger leaseCalls = new AtomicInteger();
        volatile boolean fail;
        volatile long leaseTtlMs = 60_000L; // well beyond any test's runtime
        volatile int lastRequested;
        volatile int lastUnused;

        @Override
        public LeaseGrant lease(String key, int requested, int unused, int limit, int windowSeconds) {
            leaseCalls.incrementAndGet();
            lastRequested = requested;
            lastUnused = unused;
            if (fail) {
                throw new RuntimeException("simulated Redis failure");
            }
            return new LeaseGrant(requested, System.currentTimeMillis() + leaseTtlMs, 0L);
        }
    }
}
