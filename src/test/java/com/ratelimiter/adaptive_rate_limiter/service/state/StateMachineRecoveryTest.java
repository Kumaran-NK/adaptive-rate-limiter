package com.ratelimiter.adaptive_rate_limiter.service.state;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.HealthStateMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.HealthCheckEvent;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

class StateMachineRecoveryTest {

    @Test
    void shouldTransitionFromDegradedToRecoveryWhenRedisIsHealthyEnough() {
        CircuitBreaker circuitBreaker = Mockito.mock(CircuitBreaker.class);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.OPEN);

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.getHysteresis().setEnterHealthyLatencyMs(10.0);
        properties.getHysteresis().setExitWarningLatencyMs(20.0);
        properties.getHysteresis().setDegradedStabilizationSeconds(0);
        properties.getHysteresis().setRecoveryStabilizationSeconds(0);

        HealthStateMetrics healthStateMetrics = Mockito.mock(HealthStateMetrics.class);

        StateMachine stateMachine = new StateMachine(circuitBreaker, properties, healthStateMetrics);

        HealthCheckEvent degradedEvent = new HealthCheckEvent(
                5.0,
                60.0,
                60.0,
                0.0,
                false,
                Instant.now().toEpochMilli(),
                -1
        );

        var degradedTransition = stateMachine.evaluateHealth(degradedEvent);
        assertNotNull(degradedTransition);
        assertEquals(HealthState.DEGRADED, degradedTransition.to());

        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);

        HealthCheckEvent recoveryEvent = new HealthCheckEvent(
                5.0,
                20.0,
                20.0,
                0.0,
                true,
                Instant.now().toEpochMilli(),
                -1
        );

        var recoveryTransition = stateMachine.evaluateHealth(recoveryEvent);
        assertNotNull(recoveryTransition);
        assertEquals(HealthState.RECOVERY, recoveryTransition.to());
    }

    @Test
    void shouldTransitionFromRecoveryToHealthyWhenLatencyIsWithinRecoveryThreshold() throws Exception {
        CircuitBreaker circuitBreaker = Mockito.mock(CircuitBreaker.class);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.getHysteresis().setEnterHealthyLatencyMs(10.0);
        properties.getHysteresis().setExitWarningLatencyMs(20.0);
        properties.getHysteresis().setRecoveryStabilizationSeconds(0);

        HealthStateMetrics healthStateMetrics = Mockito.mock(HealthStateMetrics.class);

        StateMachine stateMachine = new StateMachine(circuitBreaker, properties, healthStateMetrics);

        Field currentStateField = StateMachine.class.getDeclaredField("currentState");
        currentStateField.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<HealthState> currentState = (AtomicReference<HealthState>) currentStateField.get(stateMachine);
        currentState.set(HealthState.RECOVERY);

        Field stateEnteredAtField = StateMachine.class.getDeclaredField("stateEnteredAt");
        stateEnteredAtField.setAccessible(true);
        stateEnteredAtField.set(stateMachine, Instant.now().minusSeconds(2));

        HealthCheckEvent healthyEvent = new HealthCheckEvent(
                5.0,
                20.0,
                20.0,
                0.0,
                true,
                Instant.now().toEpochMilli(),
                -1
        );

        var transition = stateMachine.evaluateHealth(healthyEvent);
        assertNotNull(transition);
        assertEquals(HealthState.HEALTHY, transition.to());
    }
}