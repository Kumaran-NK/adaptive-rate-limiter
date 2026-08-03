package com.ratelimiter.adaptive_rate_limiter.service.state;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.HealthCheckEvent;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.StateTransition;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

@Service
public class StateMachine {

    private static final Logger log = LoggerFactory.getLogger(StateMachine.class);

    private final AtomicReference<HealthState> currentState =
            new AtomicReference<>(HealthState.HEALTHY);
    private final CircuitBreaker circuitBreaker;
    private final RateLimiterProperties properties;

    private Instant stateEnteredAt = Instant.now();

    public StateMachine(CircuitBreaker circuitBreaker, RateLimiterProperties properties) {
        this.circuitBreaker = circuitBreaker;
        this.properties = properties;
    }

    public HealthState getCurrentState() {
        return currentState.get();
    }

    public StateTransition evaluateHealth(HealthCheckEvent event) {
        HealthState oldState = currentState.get();
        HealthState newState = switch (oldState) {
            case HEALTHY -> evaluateFromHealthy(event);
            case WARNING -> evaluateFromWarning(event);
            case DEGRADED -> evaluateFromDegraded(event);
            case RECOVERY -> evaluateFromRecovery(event);
        };

        if (oldState != newState) {
            currentState.set(newState);
            stateEnteredAt = Instant.now();
            String reason = buildReason(oldState, newState, event);
            log.warn("STATE TRANSITION: {} → {} | Reason: {}", oldState, newState, reason);
            return new StateTransition(oldState, newState, reason, Instant.now());
        }
        return null;
    }

    private HealthState evaluateFromHealthy(HealthCheckEvent event) {
        // Fast path: circuit breaker open → straight to DEGRADED
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            return HealthState.DEGRADED;
        }

        // Hysteresis: Enter WARNING only above enterWarningLatency
        if (event.p99LatencyMs() > properties.getHysteresis().getEnterWarningLatencyMs()
                || event.errorRate() > 0.01) {
            return HealthState.WARNING;
        }
        return HealthState.HEALTHY;
    }

    private HealthState evaluateFromWarning(HealthCheckEvent event) {
        // Circuit breaker opened → DEGRADED
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            return HealthState.DEGRADED;
        }

        // Hysteresis: Exit WARNING only below exitWarningLatency AND stable
        if (event.p99LatencyMs() < properties.getHysteresis().getExitWarningLatencyMs()
                && event.errorRate() == 0
                && stableForSeconds(properties.getHysteresis().getWarningStabilizationSeconds())) {
            return HealthState.HEALTHY;
        }
        return HealthState.WARNING;
    }

    private HealthState evaluateFromDegraded(HealthCheckEvent event) {
        // If Redis is reachable and stable, transition to recovery.
        if (isHealthyEnoughForRecovery(event)) {
            // Check if circuit breaker is allowing calls (CLOSED or HALF_OPEN)
            if (circuitBreaker.getState() == CircuitBreaker.State.HALF_OPEN
                    || circuitBreaker.getState() == CircuitBreaker.State.CLOSED) {
                if (stableForSeconds(properties.getHysteresis().getDegradedStabilizationSeconds())) {
                    return HealthState.RECOVERY;
                }
            }

            // Even if circuit is OPEN, if Redis has been healthy for a while,
            // force transition to RECOVERY (the probe calls will eventually close the circuit)
            if (circuitBreaker.getState() == CircuitBreaker.State.OPEN
                    && stableForSeconds(properties.getHysteresis().getDegradedStabilizationSeconds() + 60)) {
                log.info("Redis healthy for extended period, forcing recovery despite circuit state");
                return HealthState.RECOVERY;
            }
        }
        return HealthState.DEGRADED;
    }

    private HealthState evaluateFromRecovery(HealthCheckEvent event) {
        // Fully healthy
        if (isHealthyEnoughForRecovery(event)
                && stableForSeconds(properties.getHysteresis().getRecoveryStabilizationSeconds())) {
            return HealthState.HEALTHY;
        }

        // Redis acting up again → fast exit to WARNING
        if (event.p99LatencyMs() > properties.getHysteresis().getEnterWarningLatencyMs()
                || event.errorRate() > 0.01) {
            return HealthState.WARNING;
        }
        return HealthState.RECOVERY;
    }

    private boolean isHealthyEnoughForRecovery(HealthCheckEvent event) {
        double recoveryThresholdMs = Math.max(
                properties.getHysteresis().getEnterHealthyLatencyMs(),
                properties.getHysteresis().getExitWarningLatencyMs()
        );
        return event.redisReachable()
                && event.p99LatencyMs() <= recoveryThresholdMs
                && event.errorRate() <= 0.01;
    }

    private boolean stableForSeconds(int seconds) {
        return Instant.now().isAfter(stateEnteredAt.plusSeconds(seconds));
    }

    private String buildReason(HealthState from, HealthState to, HealthCheckEvent event) {
        return String.format("P99: %.1fms, ErrorRate: %.2f%%, Redis: %s, Circuit: %s, Stable: %ds",
                event.p99LatencyMs(),
                event.errorRate() * 100,
                event.redisReachable() ? "reachable" : "unreachable",
                circuitBreaker.getState(),
                java.time.Duration.between(stateEnteredAt, Instant.now()).getSeconds());
    }
}