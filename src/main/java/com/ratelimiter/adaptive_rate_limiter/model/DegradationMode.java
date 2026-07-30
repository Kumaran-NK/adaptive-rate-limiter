package com.ratelimiter.adaptive_rate_limiter.model;

public enum DegradationMode {
    FAIL_OPEN,    // Allow requests using local counters (default for reads)
    FAIL_CLOSED,  // Block all requests when Redis is dead (for payments/SMS)
    FAIL_STRICT   // Conservative per-pod limit = total_limit / pod_count
}