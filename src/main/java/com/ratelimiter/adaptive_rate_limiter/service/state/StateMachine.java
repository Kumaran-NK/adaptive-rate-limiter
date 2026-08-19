package com.ratelimiter.adaptive_rate_limiter.service.state;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.HealthStateMetrics;
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
    private final HealthStateMetrics healthStateMetrics;

    private Instant stateEnteredAt = Instant.now();

    // Tracks the last time the shared-signal forced-recovery branch fired,
    // so a stale-but-still-valid shared timestamp can't force recovery on
    // every single health check when the circuit breaker is stuck OPEN due
    // to lack of real traffic (idle system). Without this cooldown, once the
    // shared timestamp satisfies the stability threshold it stays satisfied
    // forever (it's only refreshed, never reset), which combined with
    // evaluateFromHealthy() immediately bouncing back to DEGRADED whenever
    // the circuit is OPEN produces rapid DEGRADED<->RECOVERY<->HEALTHY
    // flapping every health-check interval.
    private Instant lastForcedRecoveryUsingSharedSignal = Instant.EPOCH;

    public StateMachine(CircuitBreaker circuitBreaker,
                         RateLimiterProperties properties,
                         HealthStateMetrics healthStateMetrics) {
        this.circuitBreaker = circuitBreaker;
        this.properties = properties;
        this.healthStateMetrics = healthStateMetrics;
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

            healthStateMetrics.updateState(newState);
            healthStateMetrics.recordTransition(oldState, newState);

            return new StateTransition(oldState, newState, reason, Instant.now());
        }
        return null;
    }

    private HealthState evaluateFromHealthy(HealthCheckEvent event) {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            return HealthState.DEGRADED;
        }
        if (event.p99LatencyMs() > properties.getHysteresis().getEnterWarningLatencyMs()
                || event.errorRate() > 0.01) {
            return HealthState.WARNING;
        }
        return HealthState.HEALTHY;
    }

    private HealthState evaluateFromWarning(HealthCheckEvent event) {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            return HealthState.DEGRADED;
        }

        if (event.p99LatencyMs() > properties.getRedisLatencyCriticalThreshold()
                && stableForSeconds(properties.getHysteresis().getWarningStabilizationSeconds())) {
            log.warn("Sustained critical latency in WARNING state — forcing DEGRADED");
            return HealthState.DEGRADED;
        }

        if (event.p99LatencyMs() < properties.getHysteresis().getExitWarningLatencyMs()
                && event.errorRate() == 0
                && stableForSeconds(properties.getHysteresis().getWarningStabilizationSeconds())) {
            return HealthState.HEALTHY;
        }
        return HealthState.WARNING;
    }

    private HealthState evaluateFromDegraded(HealthCheckEvent event) {
        if (isHealthyEnoughForRecovery(event)) {
            if (circuitBreaker.getState() == CircuitBreaker.State.HALF_OPEN
                    || circuitBreaker.getState() == CircuitBreaker.State.CLOSED) {
                if (isStableForRecovery(event, properties.getHysteresis().getDegradedStabilizationSeconds())) {
                    return HealthState.RECOVERY;
                }
            }

            if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                int requiredSeconds = properties.getHysteresis().getDegradedStabilizationSeconds() + 60;
                boolean cooldownElapsed = Instant.now().isAfter(
                        lastForcedRecoveryUsingSharedSignal.plusSeconds(requiredSeconds));

                if (isStableForRecovery(event, requiredSeconds) && cooldownElapsed) {
                    log.info("Redis healthy for extended period, forcing recovery despite circuit state");
                    lastForcedRecoveryUsingSharedSignal = Instant.now();
                    return HealthState.RECOVERY;
                }
            }
        }
        return HealthState.DEGRADED;
    }

    private HealthState evaluateFromRecovery(HealthCheckEvent event) {
        if (isHealthyEnoughForRecovery(event)
                && isStableForRecovery(event, properties.getHysteresis().getRecoveryStabilizationSeconds())) {
            return HealthState.HEALTHY;
        }

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

    private boolean isStableForRecovery(HealthCheckEvent event, int seconds) {
        if (event.sharedHealthySinceMillis() > 0) {
            long elapsedSharedMs = System.currentTimeMillis() - event.sharedHealthySinceMillis();
            if (elapsedSharedMs >= seconds * 1000L) {
                return true;
            }
        }
        return stableForSeconds(seconds);
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