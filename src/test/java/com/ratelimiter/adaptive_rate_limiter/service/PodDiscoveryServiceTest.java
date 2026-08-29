package com.ratelimiter.adaptive_rate_limiter.service;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class PodDiscoveryServiceTest {

    private final PodDiscoveryService podDiscoveryService = new PodDiscoveryService();

    @Test
    void shouldHandleZeroAndNegativePodCountSafely() {
        assertEquals(1, podDiscoveryService.parsePodCount(key -> "0"), "0 pod count must be clamped to 1");
        assertEquals(1, podDiscoveryService.parsePodCount(key -> "-5"), "Negative pod count must be clamped to 1");
    }

    @Test
    void shouldHandleNonNumericStringGracefully() {
        assertEquals(1, podDiscoveryService.parsePodCount(key -> "abc"), "Non-numeric string must fall back to default 1");
        assertEquals(1, podDiscoveryService.parsePodCount(key -> "3.14"), "Decimal string must fall back to default 1");
    }

    @Test
    void shouldParseValidPodCount() {
        assertEquals(5, podDiscoveryService.parsePodCount(key -> "5"), "Valid integer string must return parsed count");
    }

    @Test
    void shouldFallbackToReplicasWhenPodCountIsInvalidOrAbsent() {
        Map<String, String> envMap = Map.of(
                "POD_COUNT", "invalid",
                "REPLICAS", "3"
        );
        assertEquals(3, podDiscoveryService.parsePodCount(envMap::get), "Must fall back to REPLICAS when POD_COUNT is invalid");
    }

    @Test
    void shouldDefaultToOneWhenNoEnvVarIsPresent() {
        assertEquals(1, podDiscoveryService.parsePodCount(key -> null), "Default must be 1 when no env vars are present");
    }
}
