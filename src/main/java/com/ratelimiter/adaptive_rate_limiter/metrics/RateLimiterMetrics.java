package com.ratelimiter.adaptive_rate_limiter.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
public class RateLimiterMetrics {

    private final Counter requestsAllowed;
    private final Counter requestsDenied;
    private final Timer requestLatency;

    // Phase 3E -- quota-leasing observability.
    private final Counter leaseGrants;         // leases that returned quota
    private final Counter leaseRedisCallsSaved; // round trips avoided vs per-request (headline)
    private final Counter leaseWastedQuota;    // units leased but forfeited unused (over-provisioning cost)
    private final Counter leaseStarvation;     // requests that had to block on a synchronous lease
    private final AtomicInteger leaseSize = new AtomicInteger(0); // last granted lease size (gauge)

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

        this.leaseGrants = Counter.builder("rate_limiter_lease_grants_total")
                .description("Quota leases that returned at least one unit")
                .register(registry);

        this.leaseRedisCallsSaved = Counter.builder("rate_limiter_lease_redis_calls_saved_total")
                .description("Redis round trips avoided by serving admits locally from a lease")
                .register(registry);

        this.leaseWastedQuota = Counter.builder("rate_limiter_lease_wasted_quota_total")
                .description("Leased units forfeited unused when a lease expired (over-provisioning cost)")
                .register(registry);

        this.leaseStarvation = Counter.builder("rate_limiter_lease_starvation_total")
                .description("Requests that could not be served locally and had to block on a synchronous lease")
                .register(registry);

        Gauge.builder("rate_limiter_lease_size", leaseSize, AtomicInteger::get)
                .description("Size of the most recent quota lease granted")
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

    /**
     * A lease returned {@code granted} units in one Redis call. That single call
     * will serve up to {@code granted} local admits, so it saves {@code granted-1}
     * round trips versus the Phase 2 per-request path.
     */
    public void recordLeaseGrant(int granted) {
        leaseGrants.increment();
        if (granted > 1) {
            leaseRedisCallsSaved.increment(granted - 1);
        }
        leaseSize.set(granted);
    }

    /** Units leased but never spent before the lease expired. */
    public void recordWastedQuota(int units) {
        if (units > 0) {
            leaseWastedQuota.increment(units);
        }
    }

    /** A request found the local bucket empty and had to block on a synchronous lease. */
    public void recordLeaseStarvation() {
        leaseStarvation.increment();
    }
}