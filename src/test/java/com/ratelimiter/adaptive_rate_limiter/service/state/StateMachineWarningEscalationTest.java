package com.ratelimiter.adaptive_rate_limiter.service.state;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.HealthStateMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.HealthCheckEvent;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;


class StateMachineWarningEscalationTest {

    private StateMachine buildStateMachineInWarning(RateLimiterProperties properties,
                                                       CircuitBreaker circuitBreaker) throws Exception {
        HealthStateMetrics healthStateMetrics = Mockito.mock(HealthStateMetrics.class);
        StateMachine stateMachine = new StateMachine(circuitBreaker, properties, healthStateMetrics);

        Field currentStateField = StateMachine.class.getDeclaredField("currentState");
        currentStateField.setAccessible(true);
        @SuppressWarnings("unchecked")
        var currentState = (java.util.concurrent.atomic.AtomicReference<HealthState>)
                currentStateField.get(stateMachine);
        currentState.set(HealthState.WARNING);

        Field stateEnteredAtField = StateMachine.class.getDeclaredField("stateEnteredAt");
        stateEnteredAtField.setAccessible(true);
        // Backdate entry so stabilization window has already elapsed
        stateEnteredAtField.set(stateMachine, Instant.now().minusSeconds(30));

        return stateMachine;
    }

    @Test
    void shouldEscalateToDegradedWhenLatencyExceedsCriticalThresholdAndSustained() throws Exception {
        CircuitBreaker circuitBreaker = Mockito.mock(CircuitBreaker.class);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.setRedisLatencyCriticalThreshold(200);
        properties.getHysteresis().setWarningStabilizationSeconds(0); // stabilized immediately

        StateMachine stateMachine = buildStateMachineInWarning(properties, circuitBreaker);

        // p99 above critical threshold, Redis technically reachable (not a hard outage,
        // just consistently slow) -- this is the "alive but slow" case the CB alone can't catch
        HealthCheckEvent slowButReachable = new HealthCheckEvent(
                150.0,
                250.0,
                250.0,
                0.0,
                true,
                Instant.now().toEpochMilli()
        );

        var transition = stateMachine.evaluateHealth(slowButReachable);

        assertNotNull(transition, "Expected a transition out of WARNING when latency is sustained above critical threshold");
        assertEquals(HealthState.DEGRADED, transition.to());
    }

    @Test
    void shouldStayInWarningWhenLatencyElevatedButBelowCriticalThreshold() throws Exception {
        CircuitBreaker circuitBreaker = Mockito.mock(CircuitBreaker.class);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.setRedisLatencyCriticalThreshold(200);
        properties.getHysteresis().setWarningStabilizationSeconds(0);
        // exit-warning threshold left at default so this event won't satisfy exit-to-HEALTHY either

        StateMachine stateMachine = buildStateMachineInWarning(properties, circuitBreaker);

        // p99 elevated (above enter-warning) but below the critical threshold --
        // should remain in WARNING, not escalate and not recover
        HealthCheckEvent moderatelySlow = new HealthCheckEvent(
                60.0,
                80.0,
                80.0,
                0.0,
                true,
                Instant.now().toEpochMilli()
        );

        var transition = stateMachine.evaluateHealth(moderatelySlow);

        assertNull(transition, "Should remain in WARNING (no transition) when latency is elevated but under critical threshold");
    }
}
