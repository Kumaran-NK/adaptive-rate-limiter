package com.ratelimiter.adaptive_rate_limiter.service;

import com.ratelimiter.adaptive_rate_limiter.cache.LocalRateLimitCache;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.DegradationMode;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.state.StateMachine;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.TokenBucketStrategy;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final StateMachine stateMachine;
    private final SlidingWindowStrategy slidingWindowStrategy;
    private final TokenBucketStrategy tokenBucketStrategy;
    private final LocalRateLimitCache localCache;
    private final RateLimiterProperties properties;
    private final PodDiscoveryService podDiscoveryService;

    private int warningCallCounter = 0;

    public RateLimiterService(StateMachine stateMachine,
                               SlidingWindowStrategy slidingWindowStrategy,
                               TokenBucketStrategy tokenBucketStrategy,
                               LocalRateLimitCache localCache,
                               RateLimiterProperties properties,
                               PodDiscoveryService podDiscoveryService) {
        this.stateMachine = stateMachine;
        this.slidingWindowStrategy = slidingWindowStrategy;
        this.tokenBucketStrategy = tokenBucketStrategy;
        this.localCache = localCache;
        this.properties = properties;
        this.podDiscoveryService = podDiscoveryService;
    }

    public RateLimitDecision isAllowed(String key, String endpoint) {
        HealthState state = stateMachine.getCurrentState();

        return switch (state) {
            case HEALTHY -> checkWithRedis(key, endpoint);
            case WARNING -> checkWarning(key, endpoint);
            case DEGRADED -> checkDegraded(key, endpoint);
            case RECOVERY -> checkRecovery(key, endpoint);
        };
    }

    @CircuitBreaker(name = "redis", fallbackMethod = "redisFallback")
    private RateLimitDecision checkWithRedis(String key, String endpoint) {
        return slidingWindowStrategy.isAllowed(
                key,
                getLimitForEndpoint(endpoint),
                properties.getWindowSizeSeconds()
        );
    }

    private RateLimitDecision checkWarning(String key, String endpoint) {
        warningCallCounter++;

        if (warningCallCounter % 5 == 0) {
            RateLimitDecision decision = checkWithRedis(key, endpoint);
            localCache.put(key, decision.remaining());
            return decision;
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

        return checkWithRedis(key, endpoint);
    }

    private RateLimitDecision checkDegraded(String key, String endpoint) {
        DegradationMode mode = properties.getDegradationMode(endpoint);

        return switch (mode) {
            case FAIL_CLOSED -> failClosed(endpoint);
            case FAIL_STRICT -> failStrict(key, endpoint);
            case FAIL_OPEN -> failOpen(key, endpoint);
        };
    }

    private RateLimitDecision failClosed(String endpoint) {
        log.warn("FAIL_CLOSED: Blocking request for endpoint {} - Redis unavailable", endpoint);
        return RateLimitDecision.denied(
                System.currentTimeMillis(),
                HealthState.DEGRADED,
                "DEGRADED_CLOSED"
        );
    }

    private RateLimitDecision failStrict(String key, String endpoint) {
        int podCount = podDiscoveryService.getPodCount();
        int totalLimit = getLimitForEndpoint(endpoint);
        int perPodLimit = Math.max(totalLimit / podCount, 1);

        log.debug("FAIL_STRICT: Total limit {}, Pods {}, Per-pod limit {}",
                totalLimit, podCount, perPodLimit);

        return tokenBucketStrategy.isAllowed(key, perPodLimit, properties.getWindowSizeSeconds());
    }

    private RateLimitDecision failOpen(String key, String endpoint) {
        return tokenBucketStrategy.isAllowed(
                key,
                getLimitForEndpoint(endpoint),
                properties.getWindowSizeSeconds()
        );
    }

    private RateLimitDecision checkRecovery(String key, String endpoint) {
        return checkWithRedis(key, endpoint);
    }

    private RateLimitDecision redisFallback(String key, String endpoint, Throwable t) {
        log.warn("Redis call failed, falling back. Endpoint: {}", endpoint);
        return checkDegraded(key, endpoint);
    }

    public HealthState getCurrentHealthState() {
        return stateMachine.getCurrentState();
    }

    private int getLimitForEndpoint(String endpoint) {
        var endpointConfig = properties.getEndpoints().get(endpoint);
        return endpointConfig != null ? endpointConfig.getLimit() : properties.getDefaultLimit();
    }
}