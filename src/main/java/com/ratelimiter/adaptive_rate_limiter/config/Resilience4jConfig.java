package com.ratelimiter.adaptive_rate_limiter.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

@Configuration
public class Resilience4jConfig {

    private static final Logger log = LoggerFactory.getLogger(Resilience4jConfig.class);

    @Bean
    public CircuitBreaker redisCircuitBreaker(CircuitBreakerRegistry registry) {
        CircuitBreaker circuitBreaker = registry.circuitBreaker("redis");

        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> 
                    log.info("Circuit Breaker Transition: {} → {}", 
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));

        return circuitBreaker;
    }
}