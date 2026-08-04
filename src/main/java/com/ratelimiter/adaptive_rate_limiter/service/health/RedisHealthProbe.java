package com.ratelimiter.adaptive_rate_limiter.service.health;

import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.model.HealthCheckEvent;
import com.ratelimiter.adaptive_rate_limiter.service.state.AlertSuppressionService;
import com.ratelimiter.adaptive_rate_limiter.service.state.StateMachine;

@Service
public class RedisHealthProbe {

    private static final Logger log = LoggerFactory.getLogger(RedisHealthProbe.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final StateMachine stateMachine;
    private final AlertSuppressionService alertSuppression;
    private final RateLimiterProperties properties;

    private final Queue<Double> latencyWindow = new LinkedList<>();
    private final Queue<Boolean> errorWindow = new LinkedList<>();
    private static final int WINDOW_SIZE = 5;

    public RedisHealthProbe(RedisTemplate<String, String> redisTemplate,
                             StateMachine stateMachine,
                             AlertSuppressionService alertSuppression,
                             RateLimiterProperties properties) {
        this.redisTemplate = redisTemplate;
        this.stateMachine = stateMachine;
        this.alertSuppression = alertSuppression;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${rate-limiter.redis-health-check-interval:3000}",
           initialDelayString = "${rate-limiter.redis-health-check-initial-delay:5000}")
    public void checkHealth() {
        long start = System.currentTimeMillis();
        boolean reachable = false;
        double latencyMs = -1;

        try {
            String result = redisTemplate.getConnectionFactory()
                    .getConnection().ping();
            reachable = "PONG".equals(result);
            latencyMs = System.currentTimeMillis() - start;
        } catch (Exception e) {
            log.debug("Redis health check failed: {}", e.getMessage());
            reachable = false;
            latencyMs = properties.getRedisLatencyCriticalThreshold() * 2;
        }

        addToLatencyWindow(latencyMs);
        addToErrorWindow(!reachable);

        HealthCheckEvent event = new HealthCheckEvent(
                calculatePercentile(50),
                calculatePercentile(95),
                calculatePercentile(99),
                calculateErrorRate(),
                reachable,
                System.currentTimeMillis()
        );

        log.debug("Health check: P50={}ms, P99={}ms, ErrorRate={}%, Reachable={}",
                event.p50LatencyMs(), event.p99LatencyMs(),
                String.format("%.1f", event.errorRate() * 100), reachable);

        var transition = stateMachine.evaluateHealth(event);
        if (transition != null) {
            alertSuppression.onStateTransition(transition);
        }
    }

    private void addToLatencyWindow(double latency) {
        latencyWindow.add(latency);
        if (latencyWindow.size() > WINDOW_SIZE) {
            latencyWindow.poll();
        }
    }

    private void addToErrorWindow(boolean isError) {
        errorWindow.add(isError);
        if (errorWindow.size() > WINDOW_SIZE) {
            errorWindow.poll();
        }
    }

    private double calculatePercentile(int percentile) {
        if (latencyWindow.isEmpty()) return 0;
        var sorted = latencyWindow.stream().sorted().toList();
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private double calculateErrorRate() {
        if (errorWindow.isEmpty()) return 0;
        long errors = errorWindow.stream().filter(e -> e).count();
        return (double) errors / errorWindow.size();
    }
}