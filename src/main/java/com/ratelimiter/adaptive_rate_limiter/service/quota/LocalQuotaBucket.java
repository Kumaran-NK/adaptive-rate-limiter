package com.ratelimiter.adaptive_rate_limiter.service.quota;

import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;

/**
 * Local, per-key depleting quota counter for Phase 3 leasing.
 *
 * <p><b>Deliberately NOT a Token Bucket.</b> Unlike {@code TokenBucketStrategy}
 * (continuous, time-based refill), this bucket has no time-based refill at all:
 * its only token source is lease grants from Redis ({@link #topUp}). A
 * continuously refilling local bucket would let each of P pods admit at
 * {@code limit/window}, i.e. ~P x the global limit -- exactly what leasing must
 * prevent. All pacing lives in the allocator (the shared GCRA TAT); this bucket
 * only spends what was centrally granted.
 *
 * <p>Not thread-safe on its own. Callers ({@link LeaseManager}) synchronize on
 * the bucket instance, mirroring {@code TokenBucketStrategy}'s
 * {@code synchronized (bucket)} idiom.
 */
class LocalQuotaBucket {

    private double remaining;
    private int lastGranted;
    private long leaseExpiryMs;
    private boolean prefetching;
    private double pendingRefund;

    /**
     * Consumes one unit if one is available and the lease has not expired.
     * Expired tokens are not silently spent: they are moved to {@link #pendingRefund}
     * so the next lease can hand them back to the shared pool (Phase 3C refund),
     * bounding trapped quota to a lease lifetime rather than the full window.
     */
    boolean tryConsume(long nowMs) {
        if (nowMs > leaseExpiryMs) {
            pendingRefund += remaining;
            remaining = 0;
        }
        if (remaining >= 1.0) {
            remaining -= 1.0;
            return true;
        }
        return false;
    }

    /** Credits a fresh grant's units and adopts its expiry. */
    void topUp(LeaseGrant grant, long nowMs) {
        if (nowMs > leaseExpiryMs) {
            pendingRefund += remaining; // any stale remainder is refunded, not double-spent
            remaining = 0;
        }
        remaining = Math.max(0.0, remaining) + grant.granted();
        lastGranted = grant.granted();
        leaseExpiryMs = grant.leaseExpiryMs();
    }

    /** Returns and clears the units to refund to the pool on the next lease. */
    int drainRefund() {
        int refund = (int) Math.floor(pendingRefund);
        pendingRefund -= refund;
        return refund;
    }

    /** True when remaining tokens have dropped to the prefetch watermark. */
    boolean needsPrefetch(double watermark) {
        return lastGranted > 0 && remaining <= watermark * lastGranted;
    }

    /** One-in-flight-prefetch guard; caller holds {@code synchronized (bucket)}. */
    boolean beginPrefetch() {
        if (prefetching) {
            return false;
        }
        prefetching = true;
        return true;
    }

    void endPrefetch() {
        prefetching = false;
    }

    double remaining() {
        return remaining;
    }

    long leaseExpiryMs() {
        return leaseExpiryMs;
    }

    int lastGranted() {
        return lastGranted;
    }
}
