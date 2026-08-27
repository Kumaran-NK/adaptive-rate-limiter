package com.ratelimiter.adaptive_rate_limiter.model;

/**
 * The result of a single quota lease against the batch-GCRA allocator.
 *
 * <p>A pod calls {@code QuotaAllocator.lease(...)} to reserve a batch of quota
 * from Redis in one round trip, then admits requests locally against the
 * granted units (see Phase 3 design, docs/Phase-3-Adaptive-Quota-Design.md).
 *
 * @param granted         units actually leased this call, {@code 0 <= granted <= requested}.
 *                        {@code 0} means no quota was available (the caller should
 *                        keep serving from any held lease, or degrade).
 * @param leaseExpiryMs   wall-clock epoch-ms after which the granted units are
 *                        forfeited locally. Anchored to Redis' clock at grant time
 *                        plus {@code leasing.lease-ttl-ms}; bounds how long a crashed
 *                        pod's quota stays trapped.
 * @param nextAvailableMs ms until the next unit beyond {@code granted} becomes
 *                        grantable: {@code 0} if more is immediately available,
 *                        {@code > 0} if the key is now saturated.
 */
public record LeaseGrant(int granted, long leaseExpiryMs, long nextAvailableMs) {
}
