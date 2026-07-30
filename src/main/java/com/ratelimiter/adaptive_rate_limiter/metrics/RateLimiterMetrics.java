package com.ratelimiter.adaptive_rate_limiter.metrics;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
public class RateLimiterMetrics {

    private final Counter requestsAllowed;
    private final Counter requestsDenied;
    private final Timer requestLatency;

    public RateLimiterMetrics(MeterRegistry registry) {
        this.requestsAllowed = Counter.builder("rate_limiter_requests_total")
                .tag("decision", "allowed")
                .description("Total allowed requests")
                .register(registry);

        this.requestsDenied = Counter.builder("rate_limiter_requests_total")
                .tag("decision", "denied")
                .description("Total denied requests")
                .register(registry);

        this.requestLatency = Timer.builder("rate_limiter_request_duration")
                .description("Request processing latency")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public void recordAllowed() {
        requestsAllowed.increment();
    }

    public void recordDenied() {
        requestsDenied.increment();
    }

    public Timer getRequestLatency() {
        return requestLatency;
    }
}