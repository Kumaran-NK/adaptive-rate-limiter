package com.ratelimiter.adaptive_rate_limiter.config;

import com.ratelimiter.adaptive_rate_limiter.model.DegradationMode;
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
    private Map<String, EndpointConfig> endpoints = new HashMap<>();
    private DegradationMode defaultDegradationMode = DegradationMode.FAIL_OPEN;

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

    public Map<String, EndpointConfig> getEndpoints() { return endpoints; }
    public void setEndpoints(Map<String, EndpointConfig> endpoints) { this.endpoints = endpoints; }

    public DegradationMode getDefaultDegradationMode() { return defaultDegradationMode; }
    public void setDefaultDegradationMode(DegradationMode defaultDegradationMode) { this.defaultDegradationMode = defaultDegradationMode; }

    public DegradationMode getDegradationMode(String endpoint) {
        EndpointConfig config = endpoints.get(endpoint);
        return config != null ? config.getDegradationMode() : defaultDegradationMode;
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

    public static class EndpointConfig {
        private DegradationMode degradationMode = DegradationMode.FAIL_OPEN;
        private int limit = 100;

        public DegradationMode getDegradationMode() { return degradationMode; }
        public void setDegradationMode(DegradationMode degradationMode) { this.degradationMode = degradationMode; }

        public int getLimit() { return limit; }
        public void setLimit(int limit) { this.limit = limit; }
    }
}