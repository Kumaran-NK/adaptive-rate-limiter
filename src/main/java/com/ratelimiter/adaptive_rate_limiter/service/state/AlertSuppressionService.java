package com.ratelimiter.adaptive_rate_limiter.service.state;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.HealthState;
import com.ratelimiter.adaptive_rate_limiter.model.StateTransition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class AlertSuppressionService {

    private static final Logger log = LoggerFactory.getLogger(AlertSuppressionService.class);

    private final RateLimiterProperties properties;
    private final List<StateTransition> recentTransitions = new ArrayList<>();
    private Instant warningStartedAt = null;
    private boolean warningAlertSent = false;
    private boolean degradedAlertSent = false;

    public AlertSuppressionService(RateLimiterProperties properties) {
        this.properties = properties;
    }

    /**
     * Called on every state transition. Decides whether to fire an alert.
     */
    public void onStateTransition(StateTransition transition) {
        if (transition == null) return;

        recentTransitions.add(transition);
        cleanupOldTransitions();

        HealthState newState = transition.to();

        switch (newState) {
            case WARNING -> handleWarningState(transition);
            case DEGRADED -> handleDegradedState(transition);
            case RECOVERY -> handleRecoveryState(transition);
            case HEALTHY -> handleHealthyState(transition);
        }
    }

    private void handleWarningState(StateTransition transition) {
        if (warningStartedAt == null) {
            warningStartedAt = Instant.now();
            warningAlertSent = false;
            log.info("WARNING state entered. Alert will fire if sustained for {} seconds.",
                    properties.getAlert().getWarningAlertAfterSeconds());
        }

        // Check for flapping
        int flapCount = countRecentFlaps();
        if (flapCount >= properties.getAlert().getMaxFlapsBeforeAlert()) {
            log.error("ALERT: System is flapping! {} state changes in {} seconds.",
                    flapCount, properties.getAlert().getFlapWindowSeconds());
            return;
        }

        // Check for sustained warning
        if (!warningAlertSent && sustainedFor(properties.getAlert().getWarningAlertAfterSeconds())) {
            log.error("ALERT: System in WARNING state for {} seconds. P99 latency elevated.",
                    properties.getAlert().getWarningAlertAfterSeconds());
            warningAlertSent = true;
        }
    }

    private void handleDegradedState(StateTransition transition) {
        if (!degradedAlertSent) {
            log.error("ALERT: System entered DEGRADED state. Redis is unreachable. " +
                    "Rate limiting running on local fallback.");
            degradedAlertSent = true;
        }
    }

    private void handleRecoveryState(StateTransition transition) {
        log.info("RECOVERY: System is recovering. Redis is reachable again.");
    }

    private void handleHealthyState(StateTransition transition) {
        warningStartedAt = null;
        warningAlertSent = false;
        degradedAlertSent = false;
        recentTransitions.clear();
        log.info("System returned to HEALTHY state.");
    }

    private int countRecentFlaps() {
        int flapWindowMs = properties.getAlert().getFlapWindowSeconds() * 1000;
        Instant cutoff = Instant.now().minusMillis(flapWindowMs);

        return (int) recentTransitions.stream()
                .filter(t -> t.timestamp().isAfter(cutoff))
                .count();
    }

    private boolean sustainedFor(int seconds) {
        return warningStartedAt != null
                && Instant.now().isAfter(warningStartedAt.plusSeconds(seconds));
    }

    private void cleanupOldTransitions() {
        int flapWindowMs = properties.getAlert().getFlapWindowSeconds() * 1000;
        Instant cutoff = Instant.now().minusMillis(flapWindowMs);
        recentTransitions.removeIf(t -> t.timestamp().isBefore(cutoff));
    }
}