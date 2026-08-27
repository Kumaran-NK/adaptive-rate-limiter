package com.ratelimiter.adaptive_rate_limiter.service.quota;

import com.ratelimiter.adaptive_rate_limiter.model.LeaseGrant;

/**
 * Reserves a batch of quota ("a lease") from the global budget in one Redis
 * round trip, so a pod can then admit many requests locally instead of paying
 * one Redis call per request (Phase 3 quota leasing).
 *
 * <p><b>Only ever used for RATE/PACING endpoints.</b> EXACT_QUOTA endpoints
 * (payment, sms) stay on the Phase 2 direct per-request path -- leasing lets the
 * global admitted count drift above N within a window, which is unacceptable for
 * a hard, auditable ceiling. That restriction is enforced upstream in
 * {@code RateLimiterService} (strategy == GCRA), not here.
 */
public interface QuotaAllocator {

    /**
     * Attempts to lease up to {@code requested} units of quota for {@code key},
     * with no refund of previously-held quota (equivalent to {@code unused = 0}).
     *
     * @param key           the rate-limit identity (api key / client id); the
     *                      allocator namespaces it onto the same GCRA state key
     *                      the direct GCRA path uses.
     * @param requested     how many units the caller wants to lease.
     * @param limit         the endpoint's limit (units admitted per window).
     * @param windowSeconds the rate-limit window, in seconds.
     * @return a {@link LeaseGrant}; {@code granted} may be less than
     *         {@code requested} (partial grant under contention) or {@code 0}.
     */
    default LeaseGrant lease(String key, int requested, int limit, int windowSeconds) {
        return lease(key, requested, 0, limit, windowSeconds);
    }

    /**
     * Attempts to lease up to {@code requested} units, first crediting back
     * {@code unused} units the caller held from a previous lease but did not
     * spend. The refund and the new reservation happen in the SAME atomic Redis
     * round trip (no extra call): the shared GCRA TAT is first moved earlier by
     * {@code unused} emission intervals (bounded so it never precedes "now"),
     * then advanced by the freshly granted units. This bounds trapped quota to a
     * pod's lease lifetime instead of the full window.
     *
     * @param unused units from the caller's previous grant to return to the pool
     *               before granting; {@code 0} means "no refund".
     */
    LeaseGrant lease(String key, int requested, int unused, int limit, int windowSeconds);
}
