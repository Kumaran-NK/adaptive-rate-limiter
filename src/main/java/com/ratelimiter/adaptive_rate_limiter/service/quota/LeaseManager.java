package com.ratelimiter.adaptive_rate_limiter.service.quota;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.RateLimiterMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Quota-leasing admission for RATE/PACING (GCRA) endpoints -- Phase 3B.
 *
 * <p>Leases a batch of K units from Redis via {@link QuotaAllocator} (one atomic
 * batch-GCRA reservation) and then admits locally against a depleting
 * {@link LocalQuotaBucket}, so the common case costs <b>no Redis round trip</b>.
 * That is the whole point: 1 Redis call per K admits instead of 1 per admit.
 *
 * <p>Three outcomes from {@link #tryAdmit}, matching the guard in
 * {@code RateLimiterService.checkWithCircuitBreaker}:
 * <ul>
 *   <li><b>allowed</b> -- a held token was spent, or a fresh lease was drawn.</li>
 *   <li><b>denied</b> -- Redis was reached but the global budget is spent
 *       (genuine 429, {@code granted == 0}).</li>
 *   <li><b>{@code null}</b> -- Redis is unreachable / breaker OPEN; the caller
 *       degrades via its existing {@code checkDegraded} path.</li>
 * </ul>
 *
 * <p>Reuses {@code TokenBucketStrategy}'s {@code Cache<key,bucket>} +
 * {@code synchronized (bucket)} idiom, but the bucket has no time-based refill
 * (see {@link LocalQuotaBucket}). Lease size is a fixed K by default; when
 * {@code leasing.adaptive} is on, the {@link AdaptiveLeaseController} sizes it
 * from recent demand (Phase 3D).
 */
@Component
public class LeaseManager {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);
    private static final String ALGORITHM = "GCRA_LEASED";

    private final QuotaAllocator allocator;
    private final RateLimiterProperties properties;
    private final CircuitBreaker circuitBreaker;
    private final AdaptiveLeaseController controller;
    private final RateLimiterMetrics metrics;
    private final Cache<String, LocalQuotaBucket> buckets;
    private final ExecutorService prefetchExecutor;

    // Observability hooks; surfaced as Micrometer metrics in Phase 3E.
    private final AtomicLong syncLeases = new AtomicLong();      // blocking, empty-bucket leases
    private final AtomicLong prefetchLeases = new AtomicLong();  // async, ahead-of-exhaustion leases
    private final AtomicLong localAdmits = new AtomicLong();     // admits served with zero Redis calls

    public LeaseManager(QuotaAllocator allocator,
                        RateLimiterProperties properties,
                        CircuitBreakerRegistry circuitBreakerRegistry,
                        AdaptiveLeaseController controller,
                        RateLimiterMetrics metrics) {
        this.allocator = allocator;
        this.properties = properties;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("redis");
        this.controller = controller;
        this.metrics = metrics;
        this.buckets = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .maximumSize(10000)
                .build();
        this.prefetchExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "lease-prefetch");
            t.setDaemon(true);
            return t;
        });
    }

    /** Admit under the normal (HEALTHY) lease size. */
    public RateLimitDecision tryAdmit(String key, int limit, int windowSeconds) {
        return tryAdmit(key, limit, windowSeconds, false);
    }

    /**
     * @param conservative when {@code true} (RECOVERY), lease only {@code minLease}
     *                     units so a just-recovered Redis is eased back into rather
     *                     than hit with full-size batches.
     * @return an admit/deny decision, or {@code null} to signal "Redis
     *         unavailable, degrade" (never a raw exception to the hot path).
     */
    public RateLimitDecision tryAdmit(String key, int limit, int windowSeconds, boolean conservative) {
        LocalQuotaBucket bucket = buckets.get(key, k -> new LocalQuotaBucket());
        synchronized (bucket) {
            long now = System.currentTimeMillis();

            // Fast path: spend a held token; no Redis.
            if (bucket.tryConsume(now)) {
                localAdmits.incrementAndGet();
                maybePrefetch(key, bucket, limit, windowSeconds);
                return admit(bucket);
            }

            // Bucket empty/expired: draw a fresh lease synchronously through the
            // breaker, first refunding any tokens forfeited when the lease expired.
            // Reaching here means the local fast path ran dry -> this request must
            // block on Redis (starvation), the exact cost leasing works to minimize.
            metrics.recordLeaseStarvation();
            int unused = bucket.drainRefund();
            metrics.recordWastedQuota(unused); // units leased but forfeited unused before expiry
            int requested = chooseLeaseSize(key, limit, conservative);
            LeaseGrant grant = leaseThroughBreaker(key, requested, unused, limit, windowSeconds);
            if (grant == null) {
                return null; // Redis down / breaker OPEN -> degrade
            }
            syncLeases.incrementAndGet();
            recordGrantIfAdaptive(key, requested, grant.granted(), conservative);
            if (grant.granted() > 0) {
                metrics.recordLeaseGrant(grant.granted());
                bucket.topUp(grant, now);
                if (bucket.tryConsume(now)) {
                    return admit(bucket);
                }
            }
            // Reached Redis, but the global budget is spent -> genuine rate-limit denial.
            long retryAt = now + Math.max(0L, grant.nextAvailableMs());
            return RateLimitDecision.denied(retryAt, HealthState.HEALTHY, ALGORITHM);
        }
    }

    private RateLimitDecision admit(LocalQuotaBucket bucket) {
        return RateLimitDecision.allowed((int) Math.floor(bucket.remaining()),
                bucket.leaseExpiryMs(), HealthState.HEALTHY, ALGORITHM);
    }

    /**
     * Fires an async re-lease when held tokens fall to the watermark, so serving
     * never has to block on Redis under sustained load. Best-effort: any failure
     * is swallowed and the held tokens keep serving. At most one prefetch per key
     * is in flight at a time.
     */
    private void maybePrefetch(String key, LocalQuotaBucket bucket, int limit, int windowSeconds) {
        double watermark = properties.getLeasing().getPrefetchWatermark();
        if (watermark <= 0.0 || !bucket.needsPrefetch(watermark) || !bucket.beginPrefetch()) {
            return;
        }
        prefetchExecutor.submit(() -> {
            try {
                // Prefetch never refunds: those tokens are still being served locally.
                int requested = chooseLeaseSize(key, limit, false);
                LeaseGrant grant = leaseThroughBreaker(key, requested, 0, limit, windowSeconds);
                if (grant != null && grant.granted() > 0) {
                    prefetchLeases.incrementAndGet();
                    metrics.recordLeaseGrant(grant.granted());
                    recordGrantIfAdaptive(key, requested, grant.granted(), false);
                    synchronized (bucket) {
                        bucket.topUp(grant, System.currentTimeMillis());
                    }
                }
            } catch (Exception e) {
                log.debug("Prefetch lease failed for {}: {}", key, e.getMessage());
            } finally {
                synchronized (bucket) {
                    bucket.endPrefetch();
                }
            }
        });
    }

    /**
     * Leases {@code k} units (refunding {@code unused}) through the {@code "redis"}
     * circuit breaker. Returns {@code null} on any failure (breaker OPEN, timeout,
     * Redis down) so callers can degrade rather than propagate a Redis error onto
     * the request path.
     */
    private LeaseGrant leaseThroughBreaker(String key, int k, int unused, int limit, int windowSeconds) {
        try {
            return circuitBreaker.executeSupplier(
                    () -> allocator.lease(key, k, unused, limit, windowSeconds));
        } catch (Exception e) {
            log.warn("Lease acquisition failed for {} (Redis unavailable?): {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * Chooses the lease size to request. RECOVERY ({@code conservative}) always
     * takes {@code minLease} to ease a just-recovered Redis back in. Otherwise, when
     * {@code leasing.adaptive} is on, the {@link AdaptiveLeaseController} sizes the
     * lease from recent demand; when off, the fixed K of {@link #leaseSizeFor} is
     * used -- the path against which Phase 3A-3C correctness was proven.
     */
    private int chooseLeaseSize(String key, int limit, boolean conservative) {
        if (!conservative && properties.getLeasing().isAdaptive()) {
            return controller.nextLeaseSize(key, limit);
        }
        return leaseSizeFor(limit, conservative);
    }

    /**
     * Feeds the grant outcome back to the adaptive controller (partial grant ->
     * contention -> shrink). No-op unless adaptive sizing is on and this was a
     * normal (non-RECOVERY) lease, so the fixed-K and RECOVERY paths are unaffected.
     */
    private void recordGrantIfAdaptive(String key, int requested, int granted, boolean conservative) {
        if (!conservative && properties.getLeasing().isAdaptive()) {
            controller.recordGrant(key, requested, granted);
        }
    }

    /**
     * Fixed lease size K: the maximum fair share {@code maxLeaseFraction x limit},
     * clamped to {@code [minLease, limit]}. In RECOVERY ({@code conservative}) it
     * collapses to {@code minLease}. This is the default (non-adaptive) sizing; when
     * {@code leasing.adaptive} is on, {@link AdaptiveLeaseController#nextLeaseSize}
     * takes over (Phase 3D).
     */
    private int leaseSizeFor(int limit, boolean conservative) {
        RateLimiterProperties.Leasing cfg = properties.getLeasing();
        int max = Math.max(1, limit);
        if (conservative) {
            return Math.min(Math.max(1, cfg.getMinLease()), max);
        }
        int k = (int) Math.round(cfg.getMaxLeaseFraction() * limit);
        k = Math.max(cfg.getMinLease(), k);
        k = Math.min(k, max);
        return Math.max(1, k);
    }

    // --- test / metrics observability ---
    public long syncLeaseCount() {
        return syncLeases.get();
    }

    public long prefetchLeaseCount() {
        return prefetchLeases.get();
    }

    public long localAdmitCount() {
        return localAdmits.get();
    }
}
