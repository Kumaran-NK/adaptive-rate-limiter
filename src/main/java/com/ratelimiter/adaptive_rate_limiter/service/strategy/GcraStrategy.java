package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import java.util.List;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;

/**
 * GCRA (Generic Cell Rate Algorithm) rate limiting strategy.
 *
 * <p>An alternative to {@link SlidingWindowStrategy}, selected per endpoint by
 * {@code RateLimiterService} for RATE/PACING endpoints (e.g. search, ai-inference)
 * via {@code RateLimiterProperties.getStrategyForEndpoint}. EXACT_QUOTA endpoints
 * (payment, sms) stay on Sliding Window because GCRA's rolling-window count can
 * exceed the nominal limit by design (see {@code GcraInvariantTest.invariant_12}
 * and docs/GCRA-vs-Sliding-Window-Decision.md).
 *
 * <p><b>Semantics</b> (see {@code gcra.lua} for the full derivation):
 * <ul>
 *   <li>{@code emission_interval = period_ms / limit} -- the ideal spacing
 *       between conforming requests to sustain {@code limit} requests per
 *       {@code period_ms}.</li>
 *   <li>{@code delay_variation_tolerance (DVT) = period_ms} -- chosen so
 *       that burst capacity against a fully idle key is exactly
 *       {@code limit}, matching Sliding Window's "N per window" burst
 *       behavior for that specific case.</li>
 *   <li>State is a single Theoretical Arrival Time (TAT) value per key,
 *       not a set of per-request timestamps.</li>
 *   <li>The entire decision (read TAT, compare, conditionally write + set
 *       TTL) executes atomically inside one Lua script, using Redis' own
 *       clock ({@code TIME}) rather than any application server's local
 *       clock, so every instance agrees on "now".</li>
 * </ul>
 */
@Component
public class GcraStrategy implements RateLimitStrategy {

    private final RedisTemplate<String, String> redisTemplate;
    private final DefaultRedisScript<List> gcraScript;

    public GcraStrategy(RedisTemplate<String, String> redisTemplate,
                         LuaScriptLoader scriptLoader) {
        this.redisTemplate = redisTemplate;
        this.gcraScript = scriptLoader.getGcraScript();
    }

    @Override
    public RateLimitDecision isAllowed(String key, int limit, int windowSeconds) {
        String redisKey = "ratelimit:" + key + ":gcra";
        long periodMs = windowSeconds * 1000L;

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(
                gcraScript,
                List.of(redisKey),
                String.valueOf(periodMs),
                String.valueOf(limit)
        );

        if (result == null || result.size() < 4) {
            // Fail-safe: if the script somehow returns nothing usable,
            // treat this as a deny rather than silently allowing traffic
            // through an unverified GCRA decision.
            long now = System.currentTimeMillis();
            return RateLimitDecision.denied(now + periodMs, HealthState.HEALTHY, "GCRA");
        }

        boolean allowed = result.get(0) != null && result.get(0) == 1L;
        int remaining = result.get(1) != null ? result.get(1).intValue() : 0;
        long durationMs = result.get(2) != null ? result.get(2) : periodMs;

        // The Lua script returns a *duration* (ms from "now" as measured by
        // Redis), computed entirely against Redis' own clock. We convert it
        // here to an absolute epoch timestamp, using the application's
        // local clock, purely to populate RateLimitDecision.resetTimeMillis
        // in the same shape SlidingWindowStrategy uses (an absolute epoch
        // ms). This conversion is for observability/response-header
        // purposes only -- the actual accept/reject decision was already
        // made atomically inside Redis, so it is unaffected by any local
        // clock skew here.
        long now = System.currentTimeMillis();
        long resetOrRetryAt = now + durationMs;

        if (allowed) {
            return RateLimitDecision.allowed(remaining, resetOrRetryAt, HealthState.HEALTHY, "GCRA");
        } else {
            return RateLimitDecision.denied(resetOrRetryAt, HealthState.HEALTHY, "GCRA");
        }
    }

    @Override
    public String getAlgorithmName() {
        return "GCRA";
    }
}