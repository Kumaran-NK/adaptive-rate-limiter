package com.ratelimiter.adaptive_rate_limiter.config;

import com.ratelimiter.adaptive_rate_limiter.model.DegradationMode;
import com.ratelimiter.adaptive_rate_limiter.model.StrategyType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "rate-limiter")
public class RateLimiterProperties {

    private int defaultLimit = 100;
    private int windowSizeSeconds = 60;
    private int redisHealthCheckInterval = 5000;
    private int redisLatencyWarningThreshold = 50;
    private int redisLatencyCriticalThreshold = 200;
    private Cache cache = new Cache();
    private Hysteresis hysteresis = new Hysteresis();
    private Alert alert = new Alert();
    private Leasing leasing = new Leasing();
    private Map<String, EndpointConfig> endpoints = new HashMap<>();
    private DegradationMode defaultDegradationMode = DegradationMode.FAIL_OPEN;
    private StrategyType defaultStrategy = StrategyType.SLIDING_WINDOW;

    // Getters and Setters
    public int getDefaultLimit() { return defaultLimit; }
    public void setDefaultLimit(int defaultLimit) { this.defaultLimit = defaultLimit; }

    public int getWindowSizeSeconds() { return windowSizeSeconds; }
    public void setWindowSizeSeconds(int windowSizeSeconds) { this.windowSizeSeconds = windowSizeSeconds; }

    public int getRedisHealthCheckInterval() { return redisHealthCheckInterval; }
    public void setRedisHealthCheckInterval(int redisHealthCheckInterval) { this.redisHealthCheckInterval = redisHealthCheckInterval; }

    public int getRedisLatencyWarningThreshold() { return redisLatencyWarningThreshold; }
    public void setRedisLatencyWarningThreshold(int redisLatencyWarningThreshold) { this.redisLatencyWarningThreshold = redisLatencyWarningThreshold; }

    public int getRedisLatencyCriticalThreshold() { return redisLatencyCriticalThreshold; }
    public void setRedisLatencyCriticalThreshold(int redisLatencyCriticalThreshold) { this.redisLatencyCriticalThreshold = redisLatencyCriticalThreshold; }

    public Cache getCache() { return cache; }
    public void setCache(Cache cache) { this.cache = cache; }

    public Hysteresis getHysteresis() { return hysteresis; }
    public void setHysteresis(Hysteresis hysteresis) { this.hysteresis = hysteresis; }

    public Alert getAlert() { return alert; }
    public void setAlert(Alert alert) { this.alert = alert; }

    public Leasing getLeasing() { return leasing; }
    public void setLeasing(Leasing leasing) { this.leasing = leasing; }

    public Map<String, EndpointConfig> getEndpoints() { return endpoints; }
    public void setEndpoints(Map<String, EndpointConfig> endpoints) { this.endpoints = endpoints; }

    public DegradationMode getDefaultDegradationMode() { return defaultDegradationMode; }
    public void setDefaultDegradationMode(DegradationMode defaultDegradationMode) { this.defaultDegradationMode = defaultDegradationMode; }

    public DegradationMode getDegradationMode(String endpoint) {
        EndpointConfig config = endpoints.get(endpoint);
        return config != null ? config.getDegradationMode() : defaultDegradationMode;
    }

    public StrategyType getDefaultStrategy() { return defaultStrategy; }
    public void setDefaultStrategy(StrategyType defaultStrategy) { this.defaultStrategy = defaultStrategy; }

    /**
     * Resolves the distributed rate-limiting strategy for an endpoint. An endpoint
     * that does not declare its own {@code strategy} inherits {@code defaultStrategy}
     * (SLIDING_WINDOW), so existing behavior is preserved unless explicitly overridden.
     */
    public StrategyType getStrategyForEndpoint(String endpoint) {
        EndpointConfig config = endpoints.get(endpoint);
        if (config != null && config.getStrategy() != null) {
            return config.getStrategy();
        }
        return defaultStrategy;
    }

    // Inner classes
    public static class Cache {
        private int localTtlSeconds = 10;
        private int maxSize = 10000;

        public int getLocalTtlSeconds() { return localTtlSeconds; }
        public void setLocalTtlSeconds(int localTtlSeconds) { this.localTtlSeconds = localTtlSeconds; }

        public int getMaxSize() { return maxSize; }
        public void setMaxSize(int maxSize) { this.maxSize = maxSize; }
    }

    public static class Hysteresis {
        private double enterWarningLatencyMs = 50.0;
        private double exitWarningLatencyMs = 20.0;
        private double enterHealthyLatencyMs = 10.0;
        private int warningStabilizationSeconds = 60;
        private int degradedStabilizationSeconds = 120;
        private int recoveryStabilizationSeconds = 60;

        public double getEnterWarningLatencyMs() { return enterWarningLatencyMs; }
        public void setEnterWarningLatencyMs(double enterWarningLatencyMs) { this.enterWarningLatencyMs = enterWarningLatencyMs; }

        public double getExitWarningLatencyMs() { return exitWarningLatencyMs; }
        public void setExitWarningLatencyMs(double exitWarningLatencyMs) { this.exitWarningLatencyMs = exitWarningLatencyMs; }

        public double getEnterHealthyLatencyMs() { return enterHealthyLatencyMs; }
        public void setEnterHealthyLatencyMs(double enterHealthyLatencyMs) { this.enterHealthyLatencyMs = enterHealthyLatencyMs; }

        public int getWarningStabilizationSeconds() { return warningStabilizationSeconds; }
        public void setWarningStabilizationSeconds(int warningStabilizationSeconds) { this.warningStabilizationSeconds = warningStabilizationSeconds; }

        public int getDegradedStabilizationSeconds() { return degradedStabilizationSeconds; }
        public void setDegradedStabilizationSeconds(int degradedStabilizationSeconds) { this.degradedStabilizationSeconds = degradedStabilizationSeconds; }

        public int getRecoveryStabilizationSeconds() { return recoveryStabilizationSeconds; }
        public void setRecoveryStabilizationSeconds(int recoveryStabilizationSeconds) { this.recoveryStabilizationSeconds = recoveryStabilizationSeconds; }
    }

    public static class Alert {
        private int warningAlertAfterSeconds = 120;
        private int maxFlapsBeforeAlert = 5;
        private int flapWindowSeconds = 600;

        public int getWarningAlertAfterSeconds() { return warningAlertAfterSeconds; }
        public void setWarningAlertAfterSeconds(int warningAlertAfterSeconds) { this.warningAlertAfterSeconds = warningAlertAfterSeconds; }

        public int getMaxFlapsBeforeAlert() { return maxFlapsBeforeAlert; }
        public void setMaxFlapsBeforeAlert(int maxFlapsBeforeAlert) { this.maxFlapsBeforeAlert = maxFlapsBeforeAlert; }

        public int getFlapWindowSeconds() { return flapWindowSeconds; }
        public void setFlapWindowSeconds(int flapWindowSeconds) { this.flapWindowSeconds = flapWindowSeconds; }
    }

    /**
     * Phase 3 quota-leasing knobs. All default to a behavior-neutral OFF state:
     * with {@code enabled = false} the leased path is never taken and the limiter
     * behaves exactly as in Phase 2. Every value is overridable per deployment via
     * {@code rate-limiter.leasing.*}.
     */
    public static class Leasing {
        /** Master feature flag; leasing is only ever applied to RATE/PACING (GCRA) endpoints. */
        private boolean enabled = false;
        /**
         * When {@code true}, lease size is chosen by the {@code AdaptiveLeaseController}
         * (EWMA + AIMD, Phase 3D); when {@code false}, the fixed K of
         * {@code maxLeaseFraction x limit} is used. Defaults OFF so the fixed-K path --
         * against which all Phase 3A-3C correctness was proven -- stays the default.
         */
        private boolean adaptive = false;
        /** Floor on lease size K. K = 1 makes leasing reduce to the Phase 2 per-request path. */
        private int minLease = 1;
        /** Cap on K as a fraction of {@code limit} -- fairness (no pod hoards) + overshoot bound. */
        private double maxLeaseFraction = 0.25;
        /** Re-lease when remaining local tokens drop below this fraction of the last grant. */
        private double prefetchWatermark = 0.2;
        /** Adaptive controller's target time between leases (Phase 3D). */
        private int targetReleaseIntervalMs = 2000;
        /** Lease lifetime; bounds how long a crashed pod's quota stays trapped. */
        private int leaseTtlMs = 5000;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public boolean isAdaptive() { return adaptive; }
        public void setAdaptive(boolean adaptive) { this.adaptive = adaptive; }

        public int getMinLease() { return minLease; }
        public void setMinLease(int minLease) { this.minLease = minLease; }

        public double getMaxLeaseFraction() { return maxLeaseFraction; }
        public void setMaxLeaseFraction(double maxLeaseFraction) { this.maxLeaseFraction = maxLeaseFraction; }

        public double getPrefetchWatermark() { return prefetchWatermark; }
        public void setPrefetchWatermark(double prefetchWatermark) { this.prefetchWatermark = prefetchWatermark; }

        public int getTargetReleaseIntervalMs() { return targetReleaseIntervalMs; }
        public void setTargetReleaseIntervalMs(int targetReleaseIntervalMs) { this.targetReleaseIntervalMs = targetReleaseIntervalMs; }

        public int getLeaseTtlMs() { return leaseTtlMs; }
        public void setLeaseTtlMs(int leaseTtlMs) { this.leaseTtlMs = leaseTtlMs; }
    }

    public static class EndpointConfig {
        private DegradationMode degradationMode = DegradationMode.FAIL_OPEN;
        private int limit = 100;
        private StrategyType strategy;

        public DegradationMode getDegradationMode() { return degradationMode; }
        public void setDegradationMode(DegradationMode degradationMode) { this.degradationMode = degradationMode; }

        public int getLimit() { return limit; }
        public void setLimit(int limit) { this.limit = limit; }

        public StrategyType getStrategy() { return strategy; }
        public void setStrategy(StrategyType strategy) { this.strategy = strategy; }
    }
}
