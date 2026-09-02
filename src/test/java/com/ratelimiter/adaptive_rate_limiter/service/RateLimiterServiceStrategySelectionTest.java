package com.ratelimiter.adaptive_rate_limiter.service;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.cache.LocalRateLimitCache;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties.EndpointConfig;
import com.ratelimiter.adaptive_rate_limiter.metrics.RateLimiterMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.model.StrategyType;
import com.ratelimiter.adaptive_rate_limiter.service.quota.LeaseManager;
import com.ratelimiter.adaptive_rate_limiter.service.state.StateMachine;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.GcraStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.TokenBucketStrategy;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Locks in the Phase 2 wiring: {@code RateLimiterService} must pick the distributed
 * strategy from each endpoint's configured policy -- Sliding Window for EXACT_QUOTA
 * endpoints (payment, sms), GCRA for RATE/PACING endpoints (search, ai-inference),
 * and the global default (SLIDING_WINDOW) for anything unconfigured. Collaborators
 * are mocked and a real CLOSED circuit breaker executes the supplier inline, so no
 * Redis is required -- this test verifies routing, not the algorithms themselves.
 */
class RateLimiterServiceStrategySelectionTest {

    private StateMachine stateMachine;
    private SlidingWindowStrategy slidingWindow;
    private GcraStrategy gcra;
    private TokenBucketStrategy tokenBucket;
    private LocalRateLimitCache localCache;
    private RateLimiterProperties properties;
    private PodDiscoveryService podDiscovery;
    private RateLimiterMetrics metrics;
    private LeaseManager leaseManager;
    private RateLimiterService service;

    @BeforeEach
    void setUp() {
        stateMachine = mock(StateMachine.class);
        slidingWindow = mock(SlidingWindowStrategy.class);
        gcra = mock(GcraStrategy.class);
        tokenBucket = mock(TokenBucketStrategy.class);
        localCache = mock(LocalRateLimitCache.class);
        podDiscovery = mock(PodDiscoveryService.class);
        metrics = mock(RateLimiterMetrics.class);
        leaseManager = mock(LeaseManager.class);

        properties = new RateLimiterProperties();
        properties.setWindowSizeSeconds(60);
        properties.getEndpoints().put("payment", endpoint(10, StrategyType.SLIDING_WINDOW));
        properties.getEndpoints().put("search", endpoint(200, StrategyType.GCRA));

        when(stateMachine.getCurrentState()).thenReturn(HealthState.HEALTHY);
        RateLimitDecision allow = RateLimitDecision.allowed(1, 0L, HealthState.HEALTHY, "X");
        when(slidingWindow.isAllowed(anyString(), anyInt(), anyInt())).thenReturn(allow);
        when(gcra.isAllowed(anyString(), anyInt(), anyInt())).thenReturn(allow);

        service = new RateLimiterService(stateMachine, slidingWindow, gcra, tokenBucket,
                localCache, properties, podDiscovery,
                CircuitBreakerRegistry.ofDefaults(), metrics, leaseManager);
    }

    @Test
    void routesExactQuotaEndpointToSlidingWindow() {
        service.isAllowed("api-key", "payment");

        verify(slidingWindow).isAllowed("api-key", 10, 60);
        verify(gcra, never()).isAllowed(anyString(), anyInt(), anyInt());
    }

    @Test
    void routesRatePacingEndpointToGcra() {
        service.isAllowed("api-key", "search");

        verify(gcra).isAllowed("api-key", 200, 60);
        verify(slidingWindow, never()).isAllowed(anyString(), anyInt(), anyInt());
    }

    @Test
    void unconfiguredEndpointFallsBackToDefaultStrategyAndLimit() {
        service.isAllowed("api-key", "unknown");

        // Default strategy is SLIDING_WINDOW; unconfigured limit is the global default (100).
        verify(slidingWindow).isAllowed("api-key", 100, 60);
        verify(gcra, never()).isAllowed(anyString(), anyInt(), anyInt());
    }

    @Test
    void warningStateKeepsExactQuotaOnDistributedSlidingWindowPath() {
        when(stateMachine.getCurrentState()).thenReturn(HealthState.WARNING);

        service.isAllowed("api-key", "payment");

        verify(slidingWindow).isAllowed("api-key", 10, 60);
        verify(localCache, never()).tryConsume(anyString());
    }

    private static EndpointConfig endpoint(int limit, StrategyType strategy) {
        EndpointConfig config = new EndpointConfig();
        config.setLimit(limit);
        config.setStrategy(strategy);
        return config;
    }
}
