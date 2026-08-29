package com.ratelimiter.adaptive_rate_limiter.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.ratelimiter.adaptive_rate_limiter.service.quota.LeaseManager;

/**
 * Phase 3 end-to-end proof for quota leasing over the real servlet HTTP server.
 *
 * <p>This test boots the complete Spring application on a random port, points the
 * production Redis beans at a Testcontainers Redis, enables leasing only through
 * test configuration, and drives network requests through Java's HTTP client. The
 * assertions use response headers and the real {@link LeaseManager} counters to
 * prove the request path reached the leased GCRA implementation instead of the
 * direct per-request Redis strategy or a mocked collaborator.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "rate-limiter.leasing.enabled=true",
                "rate-limiter.leasing.adaptive=false",
                "rate-limiter.leasing.prefetch-watermark=0.0",
                "rate-limiter.leasing.max-lease-fraction=0.5",
                "rate-limiter.leasing.min-lease=1",
                "rate-limiter.leasing.lease-ttl-ms=30000",
                "rate-limiter.redis-health-check-initial-delay=60000",
                "rate-limiter.endpoints.search.degradation-mode=FAIL_OPEN",
                "rate-limiter.endpoints.search.limit=10",
                "rate-limiter.endpoints.search.strategy=GCRA",
                "rate-limiter.endpoints.payment.degradation-mode=FAIL_CLOSED",
                "rate-limiter.endpoints.payment.limit=10",
                "rate-limiter.endpoints.payment.strategy=SLIDING_WINDOW"
        })
@Testcontainers
class RateLimiterIntegrationTest {

    private static final int SEARCH_LIMIT = 10;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @LocalServerPort
    int port;

    @Autowired
    LeaseManager leaseManager;

    @Test
    void searchRequestsUseLeasedGcraPathServeLocallyAndEventuallyRateLimit() throws Exception {
        String apiKey = uniqueApiKey("search-leased");
        LeaseCounters before = LeaseCounters.snapshot(leaseManager);

        for (int i = 0; i < SEARCH_LIMIT; i++) {
            HttpResponse<String> response = get("/api/search", apiKey);

            assertEquals(200, response.statusCode(),
                    "search request " + i + " should be admitted before the GCRA quota is exhausted");
            assertEquals("GCRA_LEASED", header(response, "X-Algorithm-Used"),
                    "search request " + i + " must be served through the leased GCRA path");
        }

        HttpResponse<String> denied = get("/api/search", apiKey);
        assertEquals(429, denied.statusCode(),
                "one more immediate request with the same key should exhaust the leased GCRA quota");
        assertEquals("GCRA_LEASED", header(denied, "X-Algorithm-Used"),
                "the rate-limited response must still identify the leased GCRA algorithm");

        LeaseCounters delta = LeaseCounters.snapshot(leaseManager).minus(before);

        assertTrue(delta.syncLeases() > 0,
                "the leased path must acquire quota from Redis through the LeaseManager");
        assertTrue(delta.localAdmits() > 0,
                "some admitted HTTP requests must be served from LocalQuotaBucket without Redis");
        assertEquals(0, delta.prefetchLeases(),
                "prefetch is disabled in this test so Redis-call reduction is synchronous and deterministic");
        assertTrue(SEARCH_LIMIT > delta.syncLeases(),
                "admitted HTTP requests (" + SEARCH_LIMIT + ") must exceed synchronous lease acquisitions ("
                        + delta.syncLeases() + ")");
        assertTrue(SEARCH_LIMIT > delta.syncLeases() + delta.prefetchLeases(),
                "leasing must avoid one Redis acquisition per admitted request; delta=" + delta);
    }

    @Test
    void paymentRequestsRemainOnNonLeasedExactQuotaPath() throws Exception {
        String apiKey = uniqueApiKey("payment-exact");
        LeaseCounters before = LeaseCounters.snapshot(leaseManager);

        for (int i = 0; i < 3; i++) {
            HttpResponse<String> response = get("/api/payment", apiKey);

            assertEquals(200, response.statusCode(),
                    "payment request " + i + " should be admitted below its exact quota");
            String algorithm = header(response, "X-Algorithm-Used");
            assertNotNull(algorithm, "payment responses should expose the selected algorithm");
            assertFalse(algorithm.contains("LEASED"),
                    "payment is an EXACT_QUOTA endpoint and must never use leasing; algorithm=" + algorithm);
        }

        LeaseCounters after = LeaseCounters.snapshot(leaseManager);
        assertEquals(before, after,
                "payment traffic must not change LeaseManager counters even when leasing is globally enabled");
    }

    private HttpResponse<String> get(String path, String apiKey) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("X-API-Key", apiKey)
                .GET()
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String header(HttpResponse<?> response, String name) {
        return response.headers().firstValue(name).orElse(null);
    }

    private static String uniqueApiKey(String prefix) {
        return "e2e-" + prefix + "-" + UUID.randomUUID();
    }

    private record LeaseCounters(long syncLeases, long prefetchLeases, long localAdmits) {
        static LeaseCounters snapshot(LeaseManager leaseManager) {
            return new LeaseCounters(
                    leaseManager.syncLeaseCount(),
                    leaseManager.prefetchLeaseCount(),
                    leaseManager.localAdmitCount());
        }

        LeaseCounters minus(LeaseCounters earlier) {
            return new LeaseCounters(
                    syncLeases - earlier.syncLeases,
                    prefetchLeases - earlier.prefetchLeases,
                    localAdmits - earlier.localAdmits);
        }
    }
}
