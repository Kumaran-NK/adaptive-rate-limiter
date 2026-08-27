package com.ratelimiter.adaptive_rate_limiter.interceptor;

/**
 * Resolves an HTTP request URI to the logical endpoint name used as the key in
 * {@code rate-limiter.endpoints.<name>} configuration.
 *
 * <p>Fixes the endpoint-key mismatch: the interceptor previously passed the raw
 * URI (e.g. {@code /api/payment}) as the endpoint, but per-endpoint config is
 * keyed on bare logical names (e.g. {@code payment}). The raw-URI lookup always
 * missed, so every request silently fell back to the global defaults
 * ({@code default-limit}, {@code default-degradation-mode}) and no per-endpoint
 * limit or degradation mode was ever applied.
 *
 * <p>Resolution: strip the query string and surrounding slashes, drop a leading
 * {@code api} segment if present, then return the next path segment, lower-cased.
 * Examples: {@code /api/payment -> payment}, {@code /api/ai-inference ->
 * ai-inference}, {@code /api/payment/refund -> payment}, {@code / -> ""}.
 * An unconfigured result (e.g. {@code test}) still falls back to defaults
 * downstream, exactly as before -- this only fixes the names that ARE configured.
 */
public final class EndpointKeyResolver {

    private EndpointKeyResolver() {
    }

    public static String resolve(String uri) {
        if (uri == null) {
            return "";
        }
        String path = uri;
        int query = path.indexOf('?');
        if (query >= 0) {
            path = path.substring(0, query);
        }
        int start = 0;
        int end = path.length();
        while (start < end && path.charAt(start) == '/') {
            start++;
        }
        while (end > start && path.charAt(end - 1) == '/') {
            end--;
        }
        if (start >= end) {
            return "";
        }
        String[] segments = path.substring(start, end).split("/");
        int index = (segments.length > 1 && segments[0].equalsIgnoreCase("api")) ? 1 : 0;
        return segments[index].toLowerCase();
    }
}
