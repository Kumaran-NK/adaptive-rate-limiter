package com.ratelimiter.adaptive_rate_limiter.metrics;

import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class HealthStateMetrics {

    private final AtomicInteger currentState;
    private final Counter stateTransitions;

    public HealthStateMetrics(MeterRegistry registry) {
        this.currentState = registry.gauge("rate_limiter_health_state",
                new AtomicInteger(0));

        this.stateTransitions = Counter.builder("rate_limiter_state_transitions_total")
                .description("Total state transitions")
                .register(registry);

        // Initialize to HEALTHY (0)
        updateState(HealthState.HEALTHY);
    }

    public void updateState(HealthState state) {
        int stateValue = switch (state) {
            case HEALTHY -> 0;
            case WARNING -> 1;
            case DEGRADED -> 2;
            case RECOVERY -> 3;
        };
        currentState.set(stateValue);
    }

    public void recordTransition(HealthState from, HealthState to) {
        stateTransitions.increment();
    }
}