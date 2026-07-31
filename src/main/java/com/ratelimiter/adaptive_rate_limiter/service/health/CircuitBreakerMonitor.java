package com.ratelimiter.adaptive_rate_limiter.service.health;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnCallNotPermittedEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnErrorEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class CircuitBreakerMonitor {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerMonitor.class);

    private final CircuitBreaker circuitBreaker;

    public CircuitBreakerMonitor(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    @PostConstruct
    public void registerListeners() {
        circuitBreaker.getEventPublisher()
                .onStateTransition(this::onStateTransition);

        circuitBreaker.getEventPublisher()
                .onError(this::onError);

        circuitBreaker.getEventPublisher()
                .onCallNotPermitted(this::onCallNotPermitted);

        log.info("Circuit breaker event listeners registered");
    }

    private void onStateTransition(CircuitBreakerOnStateTransitionEvent event) {
        log.warn("CIRCUIT BREAKER: {} → {}",
                event.getStateTransition().getFromState(),
                event.getStateTransition().getToState());
    }

    private void onError(CircuitBreakerOnErrorEvent event) {
        log.debug("Circuit breaker recorded error: {}", event.getThrowable().getMessage());
    }

    private void onCallNotPermitted(CircuitBreakerOnCallNotPermittedEvent event) {
        log.debug("Circuit breaker blocked a call (circuit OPEN)");
    }
}