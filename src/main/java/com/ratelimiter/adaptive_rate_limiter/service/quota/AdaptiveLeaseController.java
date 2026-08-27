package com.ratelimiter.adaptive_rate_limiter.service.quota;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;

/**
 * Adaptive lease-size controller (Phase 3D) -- built LAST, after all fixed-K
 * correctness (3A-3C) is proven, so adaptive sizing never complicates debugging
 * of the core reservation/lease mechanics.
 *
 * <p>Picks the next lease size K per key from two signals:
 * <ul>
 *   <li><b>EWMA of consumption rate</b> -- an exponentially-weighted estimate of
 *       how fast this key drains units (units/sec), giving a rate-based target
 *       {@code rate x targetReleaseIntervalMs/1000}: "lease roughly enough to
 *       cover the next release interval at the current rate."</li>
 *   <li><b>AIMD</b> -- additive-increase when a lease drains within the target
 *       interval (sustained demand), multiplicative-decrease when a lease drains
 *       slower than target (easing / over-provisioned) or the allocator returns a
 *       partial grant (global contention). Additive growth + multiplicative
 *       backoff is the same fairness-friendly control law TCP uses.</li>
 * </ul>
 *
 * <p>K is always clamped to {@code [minLease, round(maxLeaseFraction x limit)]},
 * so the per-pod overshoot bound proven for the fixed-K path
 * ({@code <= P x maxLeaseFraction x limit}) still holds under adaptivity.
 *
 * <p>The drain-time signal is derived from the wall-clock gap between successive
 * {@link #nextLeaseSize} calls for a key (a new lease is drawn only when the
 * previous batch is exhausted or hits the prefetch watermark), so a lease that
 * "expires with unused tokens" surfaces here as a long idle gap -> the
 * slow-drain branch shrinks K, exactly as the design's over-provision signal
 * intends. The clock is injectable so tests are deterministic without sleeps.
 */
@Component
public class AdaptiveLeaseController {

    /** EWMA smoothing factor: weight on the newest rate sample. */
    private static final double EWMA_ALPHA = 0.3;
    /** AIMD additive increase, in units, applied per sustained-demand lease. */
    private static final double INCREASE_STEP = 1.0;
    /** AIMD multiplicative decrease, applied on easing / partial grant. */
    private static final double DECREASE_FACTOR = 0.5;

    private final RateLimiterProperties properties;
    private final LongSupplier clock;
    private final Cache<String, Stats> stats;

    @Autowired
    public AdaptiveLeaseController(RateLimiterProperties properties) {
        this(properties, System::currentTimeMillis);
    }

    /** Package-private for deterministic tests: supply a controllable clock. */
    AdaptiveLeaseController(RateLimiterProperties properties, LongSupplier clock) {
        this.properties = properties;
        this.clock = clock;
        this.stats = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .maximumSize(10000)
                .build();
    }

    /**
     * The lease size to request now for {@code key}, adapting to its recent demand
     * and clamped to {@code [minLease, maxLeaseFraction x limit]}.
     */
    public int nextLeaseSize(String key, int limit) {
        RateLimiterProperties.Leasing cfg = properties.getLeasing();
        int minLease = Math.max(1, cfg.getMinLease());
        int maxK = Math.max(minLease, (int) Math.round(cfg.getMaxLeaseFraction() * limit));
        long now = clock.getAsLong();

        Stats s = stats.get(key, k -> new Stats(minLease));
        synchronized (s) {
            if (s.lastLeaseMs > 0) {
                long elapsed = Math.max(1L, now - s.lastLeaseMs);
                // EWMA of the observed drain rate (units the last lease released / sec).
                double instRate = s.lastSize / (elapsed / 1000.0);
                s.ewmaRate = EWMA_ALPHA * instRate + (1 - EWMA_ALPHA) * s.ewmaRate;
                double rateTarget = s.ewmaRate * cfg.getTargetReleaseIntervalMs() / 1000.0;

                if (elapsed <= cfg.getTargetReleaseIntervalMs()) {
                    // Drained within the target window -> sustained demand -> grow:
                    // additive floor, but jump to the rate estimate if it is higher.
                    s.k = Math.max(s.k + INCREASE_STEP, rateTarget);
                } else {
                    // Drained slower than target (or lease sat idle until expiry) ->
                    // over-provisioned -> multiplicative backoff.
                    s.k = s.k * DECREASE_FACTOR;
                }
            }
            s.k = clamp(s.k, minLease, maxK);
            int size = (int) Math.round(s.k);
            size = Math.max(minLease, Math.min(size, maxK));
            s.lastSize = size;
            s.lastLeaseMs = now;
            return size;
        }
    }

    /**
     * Feedback from the allocator: a partial grant ({@code granted < requested})
     * means the global budget is contended, so back K off multiplicatively. Also
     * records the actual granted count so the next EWMA sample reflects reality.
     */
    public void recordGrant(String key, int requested, int granted) {
        Stats s = stats.getIfPresent(key);
        if (s == null) {
            return;
        }
        synchronized (s) {
            s.lastSize = granted;
            if (granted < requested) {
                s.k = Math.max(1.0, s.k * DECREASE_FACTOR);
            }
        }
    }

    /** Test/metrics observability: the current (unrounded) K estimate for a key. */
    double currentK(String key) {
        Stats s = stats.getIfPresent(key);
        if (s == null) {
            return 0.0;
        }
        synchronized (s) {
            return s.k;
        }
    }

    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(v, hi));
    }

    /** Per-key adaptive state. Guarded by {@code synchronized (this instance)}. */
    private static final class Stats {
        private double k;          // current lease-size estimate (unrounded)
        private double ewmaRate;   // EWMA of consumption rate, units/sec
        private int lastSize;      // last size handed out (or last granted)
        private long lastLeaseMs;  // clock at the previous nextLeaseSize call

        Stats(int initialK) {
            this.k = initialK;
        }
    }
}
