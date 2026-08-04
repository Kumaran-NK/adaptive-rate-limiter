package com.ratelimiter.adaptive_rate_limiter.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PodDiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(PodDiscoveryService.class);

    /**
     * Returns the number of running pods, read from the POD_COUNT or REPLICAS
     * environment variable (expected to be injected via the Kubernetes downward
     * API in a real deployment). Falls back to 1 for local development, where
     * no such variable is set.
     */
    public int getPodCount() {
        String podCount = System.getenv("POD_COUNT");
        if (podCount != null && !podCount.isEmpty()) {
            return Integer.parseInt(podCount);
        }

        String replicas = System.getenv("REPLICAS");
        if (replicas != null && !replicas.isEmpty()) {
            return Integer.parseInt(replicas);
        }

        log.debug("Pod count not available, defaulting to 1");
        return 1;
    }
}