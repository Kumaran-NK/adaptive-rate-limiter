package com.ratelimiter.adaptive_rate_limiter.service.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ratelimiter.adaptive_rate_limiter.model.StateTransition;

@Component
public class StateTransitionLogger {

    private static final Logger log = LoggerFactory.getLogger(StateTransitionLogger.class);

    public void logTransition(StateTransition transition) {
        if (transition == null) return;
        
        log.warn("STATE CHANGE: {} → {} | Reason: {} | Time: {}",
                transition.from(),
                transition.to(),
                transition.reason(),
                transition.timestamp());
    }
}