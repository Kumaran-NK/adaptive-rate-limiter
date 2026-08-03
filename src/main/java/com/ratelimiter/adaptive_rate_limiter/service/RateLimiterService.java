package com.ratelimiter.adaptive_rate_limiter.service;

import com.ratelimiter.adaptive_rate_limiter.cache.LocalRateLimitCache;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.DegradationMode;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.state.StateMachine;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.TokenBucketStrategy;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final StateMachine stateMachine;
    private final SlidingWindowStrategy slidingWindowStrategy;
    private final TokenBucketStrategy tokenBucketStrategy;
    private final LocalRateLimitCache localCache;
    private final RateLimiterProperties properties;
    private final PodDiscoveryService podDiscoveryService;
    private final CircuitBreaker circuitBreaker;

    private int warningCallCounter = 0;
    private int degradedProbeCounter = 0;

    public RateLimiterService(StateMachine stateMachine,
                               SlidingWindowStrategy slidingWindowStrategy,
                               TokenBucketStrategy tokenBucketStrategy,
                               LocalRateLimitCache localCache,
                               RateLimiterProperties properties,
                               PodDiscoveryService podDiscoveryService,
                               CircuitBreakerRegistry circuitBreakerRegistry) {
        this.stateMachine = stateMachine;
        this.slidingWindowStrategy = slidingWindowStrategy;
        this.tokenBucketStrategy = tokenBucketStrategy;
        this.localCache = localCache;
        this.properties = properties;
        this.podDiscoveryService = podDiscoveryService;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("redis");
    }

    public RateLimitDecision isAllowed(String key, String endpoint) {
        HealthState state = stateMachine.getCurrentState();

        return switch (state) {
            case HEALTHY -> checkWithCircuitBreaker(key, endpoint);
            case WARNING -> checkWarning(key, endpoint);
            case DEGRADED -> checkDegraded(key, endpoint);
            case RECOVERY -> checkRecovery(key, endpoint);
        };
    }

    private RateLimitDecision checkWithCircuitBreaker(String key, String endpoint) {
        Supplier<RateLimitDecision> redisCall = () ->
                slidingWindowStrategy.isAllowed(key, getLimitForEndpoint(endpoint), properties.getWindowSizeSeconds());

        try {
            return circuitBreaker.executeSupplier(redisCall);
        } catch (Exception e) {
            log.warn("Circuit breaker prevented Redis call: {}", e.getMessage());
            return checkDegraded(key, endpoint);
        }
    }

    private RateLimitDecision checkWarning(String key, String endpoint) {
        warningCallCounter++;

        if (warningCallCounter % 5 == 0) {
            return checkWithCircuitBreaker(key, endpoint);
        }

        Integer cachedValue = localCache.get(key);
        if (cachedValue != null && cachedValue > 0) {
            localCache.put(key, cachedValue - 1);
            return RateLimitDecision.allowed(
                    cachedValue - 1,
                    System.currentTimeMillis() + properties.getWindowSizeSeconds() * 1000L,
                    HealthState.WARNING,
                    "SLIDING_WINDOW_CACHED"
            );
        }

        return checkWithCircuitBreaker(key, endpoint);
    }

    private RateLimitDecision checkDegraded(String key, String endpoint) {
        // PROBE: Every 10th request, try Redis to see if it's back
        degradedProbeCounter++;
        if (degradedProbeCounter % 10 == 0) {
            log.debug("DEGRADED probe: Testing if Redis is back...");
            try {
                RateLimitDecision redisDecision = checkWithCircuitBreaker(key, endpoint);
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
        return checkWithCircuitBreaker(key, endpoint);
    }

    public HealthState getCurrentHealthState() {
        return stateMachine.getCurrentState();
    }

    private int getLimitForEndpoint(String endpoint) {
        var endpointConfig = properties.getEndpoints().get(endpoint);
        return endpointConfig != null ? endpointConfig.getLimit() : properties.getDefaultLimit();
    }
}