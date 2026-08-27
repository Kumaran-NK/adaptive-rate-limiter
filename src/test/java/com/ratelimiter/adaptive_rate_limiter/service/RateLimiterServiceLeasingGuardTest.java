package com.ratelimiter.adaptive_rate_limiter.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import com.ratelimiter.adaptive_rate_limiter.model.DegradationMode;
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
 * Phase 3C -- the hard EXACT_QUOTA guard and the leasing degrade fall-through,
 * verified with mocked collaborators (no Redis). With {@code leasing.enabled=true}:
 * <ul>
 *   <li>an EXACT_QUOTA endpoint (payment / SLIDING_WINDOW) MUST NOT touch the
 *       {@link LeaseManager} -- it always takes the direct per-request path;</li>
 *   <li>a RATE/PACING endpoint (search / GCRA) is served by the LeaseManager;</li>
 *   <li>when the LeaseManager signals degrade ({@code null} = Redis down), the
 *       service falls through to its existing degraded Token Bucket path, without
 *       propagating a raw error.</li>
 * </ul>
 */
class RateLimiterServiceLeasingGuardTest {

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
        properties.getLeasing().setEnabled(true); // leasing ON for all these tests
        properties.getEndpoints().put("payment",
                endpoint(10, StrategyType.SLIDING_WINDOW, DegradationMode.FAIL_CLOSED));
        properties.getEndpoints().put("search",
                endpoint(200, StrategyType.GCRA, DegradationMode.FAIL_OPEN));

        when(stateMachine.getCurrentState()).thenReturn(HealthState.HEALTHY);
        RateLimitDecision allow = RateLimitDecision.allowed(1, 0L, HealthState.HEALTHY, "X");
        when(slidingWindow.isAllowed(anyString(), anyInt(), anyInt())).thenReturn(allow);
        when(gcra.isAllowed(anyString(), anyInt(), anyInt())).thenReturn(allow);

        service = new RateLimiterService(stateMachine, slidingWindow, gcra, tokenBucket,
                localCache, properties, podDiscovery,
                CircuitBreakerRegistry.ofDefaults(), metrics, leaseManager);
    }

    @Test
    void exactQuotaEndpointNeverLeasesEvenWhenLeasingEnabled() {
        service.isAllowed("api-key", "payment");

        // The hard guard: an EXACT_QUOTA endpoint must never reach the LeaseManager.
        verify(leaseManager, never()).tryAdmit(anyString(), anyInt(), anyInt(), anyBoolean());
        verify(leaseManager, never()).tryAdmit(anyString(), anyInt(), anyInt());
        verify(slidingWindow).isAllowed("api-key", 10, 60);
    }

    @Test
    void ratePacingEndpointIsServedByLeaseManager() {
        when(leaseManager.tryAdmit("api-key", 200, 60, false))
                .thenReturn(RateLimitDecision.allowed(199, 0L, HealthState.HEALTHY, "GCRA_LEASED"));

        RateLimitDecision decision = service.isAllowed("api-key", "search");

        verify(leaseManager).tryAdmit("api-key", 200, 60, false);
        verify(gcra, never()).isAllowed(anyString(), anyInt(), anyInt());
        assertEquals("GCRA_LEASED", decision.algorithmUsed());
        assertTrue(decision.allowed());
    }

    @Test
    void leaseDegradeSignalFallsThroughToDegradedTokenBucket() {
        // null = Redis unavailable -> the service must degrade, not error.
        when(leaseManager.tryAdmit("api-key", 200, 60, false)).thenReturn(null);
        when(tokenBucket.isAllowed(anyString(), anyInt(), anyInt()))
                .thenReturn(RateLimitDecision.allowed(1, 0L, HealthState.DEGRADED, "TOKEN_BUCKET"));

        RateLimitDecision decision = service.isAllowed("api-key", "search");

        verify(leaseManager).tryAdmit("api-key", 200, 60, false);
        // search degrades FAIL_OPEN -> Token Bucket at the full endpoint limit.
        verify(tokenBucket).isAllowed("api-key", 200, 60);
        verify(gcra, never()).isAllowed(anyString(), anyInt(), anyInt());
        assertEquals(HealthState.DEGRADED, decision.systemHealth());
    }

    private static EndpointConfig endpoint(int limit, StrategyType strategy, DegradationMode mode) {
        EndpointConfig config = new EndpointConfig();
        config.setLimit(limit);
        config.setStrategy(strategy);
        config.setDegradationMode(mode);
        return config;
    }
}
