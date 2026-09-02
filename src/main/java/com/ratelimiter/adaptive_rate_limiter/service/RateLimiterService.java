package com.ratelimiter.adaptive_rate_limiter.service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ratelimiter.adaptive_rate_limiter.cache.LocalRateLimitCache;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.RateLimiterMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.DegradationMode;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.model.StrategyType;
import com.ratelimiter.adaptive_rate_limiter.service.quota.LeaseManager;
import com.ratelimiter.adaptive_rate_limiter.service.state.StateMachine;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.GcraStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.RateLimitStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.TokenBucketStrategy;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final StateMachine stateMachine;
    private final SlidingWindowStrategy slidingWindowStrategy;
    private final GcraStrategy gcraStrategy;
    private final TokenBucketStrategy tokenBucketStrategy;
    private final LocalRateLimitCache localCache;
    private final RateLimiterProperties properties;
    private final PodDiscoveryService podDiscoveryService;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiterMetrics rateLimiterMetrics;
    private final LeaseManager leaseManager;

    private final AtomicInteger warningCallCounter = new AtomicInteger();
    private final AtomicInteger degradedProbeCounter = new AtomicInteger();

    public RateLimiterService(StateMachine stateMachine,
                               SlidingWindowStrategy slidingWindowStrategy,
                               GcraStrategy gcraStrategy,
                               TokenBucketStrategy tokenBucketStrategy,
                               LocalRateLimitCache localCache,
                               RateLimiterProperties properties,
                               PodDiscoveryService podDiscoveryService,
                               CircuitBreakerRegistry circuitBreakerRegistry,
                               RateLimiterMetrics rateLimiterMetrics,
                               LeaseManager leaseManager) {
        this.stateMachine = stateMachine;
        this.slidingWindowStrategy = slidingWindowStrategy;
        this.gcraStrategy = gcraStrategy;
        this.tokenBucketStrategy = tokenBucketStrategy;
        this.localCache = localCache;
        this.properties = properties;
        this.podDiscoveryService = podDiscoveryService;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("redis");
        this.rateLimiterMetrics = rateLimiterMetrics;
        this.leaseManager = leaseManager;
    }

    public RateLimitDecision isAllowed(String key, String endpoint) {
        HealthState state = stateMachine.getCurrentState();

        RateLimitDecision decision = switch (state) {
            case HEALTHY -> checkWithCircuitBreaker(key, endpoint);
            case WARNING -> checkWarning(key, endpoint);
            case DEGRADED -> checkDegraded(key, endpoint);
            case RECOVERY -> checkRecovery(key, endpoint);
        };

        if (decision.allowed()) {
            rateLimiterMetrics.recordAllowed();
        } else {
            rateLimiterMetrics.recordDenied();
        }
        return decision;
    }

    private RateLimitDecision checkWithCircuitBreaker(String key, String endpoint) {
        return checkDistributed(key, endpoint, false);
    }

    private RateLimitDecision checkDistributed(String key, String endpoint, boolean recovering) {
        // Quota-leasing fast path -- RATE/PACING (GCRA) endpoints only. The
        // `== GCRA` test IS the hard EXACT_QUOTA guard: payment/sms resolve to
        // SLIDING_WINDOW, so they can never reach the leased path and always take
        // the per-request distributed call below. A null decision means Redis is
        // unavailable, so we degrade exactly as the distributed path would. During
        // RECOVERY the manager leases conservatively (minLease) via `recovering`.
        if (properties.getLeasing().isEnabled()
                && properties.getStrategyForEndpoint(endpoint) == StrategyType.GCRA) {
            RateLimitDecision leased = leaseManager.tryAdmit(
                    key, getLimitForEndpoint(endpoint), properties.getWindowSizeSeconds(), recovering);
            if (leased != null) {
                localCache.put(key, leased.remaining());
                return leased;
            }
            return checkDegraded(key, endpoint);
        }

        RateLimitStrategy distributedStrategy = distributedStrategyFor(endpoint);
        Supplier<RateLimitDecision> redisCall = () ->
                distributedStrategy.isAllowed(key, getLimitForEndpoint(endpoint), properties.getWindowSizeSeconds());

        try {
            RateLimitDecision decision = circuitBreaker.executeSupplier(redisCall);
            // Seed the local cache so WARNING-state reads have real data instead
            // of always missing and falling through to checkWithCircuitBreaker.
            localCache.put(key, decision.remaining());
            return decision;
        } catch (Exception e) {
            log.warn("Circuit breaker prevented Redis call: {}", e.getMessage());
            return checkDegraded(key, endpoint);
        }
    }

    private RateLimitDecision checkWarning(String key, String endpoint) {
        // EXACT_QUOTA endpoints must preserve their hard rolling-window
        // guarantee even while Redis is slow. The local cache is intentionally
        // approximate, so it is only eligible for GCRA/RATE-PACING endpoints.
        if (properties.getStrategyForEndpoint(endpoint) != StrategyType.GCRA) {
            return checkWithCircuitBreaker(key, endpoint);
        }

        int count = warningCallCounter.incrementAndGet();

        if (count % 5 == 0) {
            return checkWithCircuitBreaker(key, endpoint);
        }

        LocalRateLimitCache.ConsumeResult consumeResult = localCache.tryConsume(key);
        if (consumeResult.allowed()) {
            return RateLimitDecision.allowed(
                    consumeResult.remaining(),
                    System.currentTimeMillis() + properties.getWindowSizeSeconds() * 1000L,
                    HealthState.WARNING,
                    properties.getStrategyForEndpoint(endpoint).name() + "_CACHED"
            );
        }

        return checkWithCircuitBreaker(key, endpoint);
    }

    private RateLimitDecision checkDegraded(String key, String endpoint) {
        // PROBE: Every 10th request, try Redis directly through the circuit breaker
        // to see if it's back. This deliberately does NOT go through
        // checkWithCircuitBreaker(), because that method swallows
        // CallNotPermittedException and recurses back into checkDegraded(),
        // which previously caused this probe to log a false "SUCCESS" even
        // when the circuit breaker blocked the call and Redis was never
        // actually reached. Guarding on circuit state here avoids attempting
        // (and misreporting) a probe we already know will be blocked.
        int count = degradedProbeCounter.incrementAndGet();
        if (count % 10 == 0 && circuitBreaker.getState() != CircuitBreaker.State.OPEN) {
            log.debug("DEGRADED probe: Testing if Redis is back...");
            try {
                RateLimitDecision redisDecision = circuitBreaker.executeSupplier(() ->
                        distributedStrategyFor(endpoint).isAllowed(key, getLimitForEndpoint(endpoint), properties.getWindowSizeSeconds()));
                localCache.put(key, redisDecision.remaining());
                log.info("DEGRADED probe SUCCESS: Redis is reachable!");
                return redisDecision;
            } catch (Exception e) {
                log.debug("DEGRADED probe failed: Redis still down");
                // Fall through to local fallback
            }
        }

        // Normal degraded operation
        DegradationMode mode = properties.getDegradationMode(endpoint);

        return switch (mode) {
            case FAIL_CLOSED -> {
                log.warn("FAIL_CLOSED: Blocking {} - Redis unavailable", endpoint);
                yield RateLimitDecision.denied(
                        System.currentTimeMillis(),
                        HealthState.DEGRADED,
                        "DEGRADED_CLOSED"
                );
            }
            case FAIL_STRICT -> {
                int podCount = podDiscoveryService.getPodCount();
                int totalLimit = getLimitForEndpoint(endpoint);
                int perPodLimit = Math.max(totalLimit / podCount, 1);
                yield tokenBucketStrategy.isAllowed(key, perPodLimit, properties.getWindowSizeSeconds());
            }
            case FAIL_OPEN -> tokenBucketStrategy.isAllowed(
                    key, getLimitForEndpoint(endpoint), properties.getWindowSizeSeconds()
            );
        };
    }

    private RateLimitDecision checkRecovery(String key, String endpoint) {
        // Same distributed path as HEALTHY, but leases conservatively (minLease)
        // so a just-recovered Redis is eased back into rather than hit with full
        // batches. Applies only to leased GCRA endpoints; others are unaffected.
        return checkDistributed(key, endpoint, true);
    }

    public HealthState getCurrentHealthState() {
        return stateMachine.getCurrentState();
    }

    /**
     * Selects the distributed strategy for an endpoint from its configured policy:
     * EXACT_QUOTA endpoints (payment, sms) resolve to Sliding Window; RATE/PACING
     * endpoints (search, ai-inference) resolve to GCRA. Unconfigured endpoints fall
     * back to the global default (SLIDING_WINDOW). This is the only place the two
     * distributed algorithms are chosen between -- a data-driven policy lookup, not
     * a hardcoded toggle. Token Bucket remains the local degraded-mode fast path.
     */
    private RateLimitStrategy distributedStrategyFor(String endpoint) {
        return properties.getStrategyForEndpoint(endpoint) == StrategyType.GCRA
                ? gcraStrategy
                : slidingWindowStrategy;
    }

    private int getLimitForEndpoint(String endpoint) {
        var endpointConfig = properties.getEndpoints().get(endpoint);
        return endpointConfig != null ? endpointConfig.getLimit() : properties.getDefaultLimit();
    }
}
