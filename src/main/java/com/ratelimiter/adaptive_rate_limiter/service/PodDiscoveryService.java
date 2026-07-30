package com.ratelimiter.adaptive_rate_limiter.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PodDiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(PodDiscoveryService.class);

    /**
     * Returns the number of running pods in the cluster.
     * In Kubernetes, this reads from the Kubernetes API.
     * For local development, it returns a default value.
     */
    public int getPodCount() {
        // Try Kubernetes API first
        String podCount = System.getenv("POD_COUNT");
        if (podCount != null && !podCount.isEmpty()) {
            return Integer.parseInt(podCount);
        }

        // Try Kubernetes downward API
        String replicas = System.getenv("REPLICAS");
        if (replicas != null && !replicas.isEmpty()) {
            return Integer.parseInt(replicas);
        }

        // Default for local development
        log.debug("Pod count not available, defaulting to 1");
        return 1;
    }
}