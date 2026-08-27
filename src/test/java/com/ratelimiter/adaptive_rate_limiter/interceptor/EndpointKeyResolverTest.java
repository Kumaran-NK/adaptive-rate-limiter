package com.ratelimiter.adaptive_rate_limiter.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Locks in the endpoint-key fix: every configured endpoint name in
 * application.properties must be recoverable from its {@code /api/...} route,
 * otherwise per-endpoint limit + degradation config silently reverts to defaults.
 */
class EndpointKeyResolverTest {

    @Test
    void stripsApiPrefixToLogicalName() {
        assertEquals("payment", EndpointKeyResolver.resolve("/api/payment"));
        assertEquals("search", EndpointKeyResolver.resolve("/api/search"));
        assertEquals("ai-inference", EndpointKeyResolver.resolve("/api/ai-inference"));
    }

    @Test
    void matchesEveryConfiguredEndpointKey() {
        // Exactly the keys under rate-limiter.endpoints.* in application.properties.
        assertEquals("payment", EndpointKeyResolver.resolve("/api/payment"));
        assertEquals("sms", EndpointKeyResolver.resolve("/api/sms"));
        assertEquals("ai-inference", EndpointKeyResolver.resolve("/api/ai-inference"));
        assertEquals("search", EndpointKeyResolver.resolve("/api/search"));
    }

    @Test
    void usesFirstSegmentAfterApiForNestedPaths() {
        assertEquals("payment", EndpointKeyResolver.resolve("/api/payment/refund"));
    }

    @Test
    void handlesMissingApiPrefix() {
        assertEquals("payment", EndpointKeyResolver.resolve("/payment"));
    }

    @Test
    void stripsQueryStringAndTrailingSlash() {
        assertEquals("search", EndpointKeyResolver.resolve("/api/search/?q=foo"));
        assertEquals("search", EndpointKeyResolver.resolve("/api/search?q=foo"));
        assertEquals("search", EndpointKeyResolver.resolve("/api/search/"));
    }

    @Test
    void lowercasesForStableLookup() {
        assertEquals("payment", EndpointKeyResolver.resolve("/API/Payment"));
    }

    @Test
    void emptyRootAndNullResolveToEmpty() {
        assertEquals("", EndpointKeyResolver.resolve("/"));
        assertEquals("", EndpointKeyResolver.resolve(""));
        assertEquals("", EndpointKeyResolver.resolve(null));
    }
}
