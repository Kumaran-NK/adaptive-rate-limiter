package com.ratelimiter.adaptive_rate_limiter.model;

/**
 * Distributed rate-limiting algorithm selected per endpoint via
 * {@code rate-limiter.endpoints.<name>.strategy}.
 *
 * <p>The choice is driven by the endpoint's semantic class, not performance
 * (see docs/GCRA-vs-Sliding-Window-Decision.md):
 * <ul>
 *   <li>{@link #SLIDING_WINDOW} -- EXACT_QUOTA endpoints whose limit is a hard,
 *       auditable ceiling (payment, sms). Guarantees no more than N admits in any
 *       rolling window.</li>
 *   <li>{@link #GCRA} -- RATE/PACING endpoints that shield a downstream from
 *       sustained overload (search, ai-inference). Burst allowance + smooth pacing;
 *       O(1) memory per key. Its rolling-window count can exceed N by design.</li>
 * </ul>
 */
public enum StrategyType {
    SLIDING_WINDOW,
    GCRA
}
