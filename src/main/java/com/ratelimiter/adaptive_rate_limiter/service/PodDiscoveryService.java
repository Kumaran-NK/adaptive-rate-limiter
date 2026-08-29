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
        return parsePodCount(System::getenv);
    }

    int parsePodCount(java.util.function.Function<String, String> envResolver) {
        String podCountStr = envResolver.apply("POD_COUNT");
        if (podCountStr != null && !podCountStr.isEmpty()) {
            try {
                return Math.max(1, Integer.parseInt(podCountStr));
            } catch (NumberFormatException e) {
                log.warn("Invalid POD_COUNT environment variable: {}", podCountStr);
            }
        }

        String replicasStr = envResolver.apply("REPLICAS");
        if (replicasStr != null && !replicasStr.isEmpty()) {
            try {
                return Math.max(1, Integer.parseInt(replicasStr));
            } catch (NumberFormatException e) {
                log.warn("Invalid REPLICAS environment variable: {}", replicasStr);
            }
        }

        log.debug("Pod count not available or invalid, defaulting to 1");
        return 1;
    }
}