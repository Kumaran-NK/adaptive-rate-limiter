package com.ratelimiter.adaptive_rate_limiter.service.health;

import java.time.Duration;
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

    // Shared, fleet-wide "Redis has been healthy since X" signal. Any instance
    // may set it; the first one to observe recovery wins (SETNX-style), so all
    // instances measure recovery stability from the same wall-clock moment
    // instead of each instance's own, possibly-later, first observation.
    private static final String HEALTHY_SINCE_KEY = "ratelimit:global:healthy-since";
    private static final int HEALTHY_SIGNAL_TTL_SECONDS = 120;

    private final RedisTemplate<String, String> redisTemplate;
    private final StateMachine stateMachine;
    private final AlertSuppressionService alertSuppression;
    private final RateLimiterProperties properties;

    private final Queue<Double> latencyWindow = new LinkedList<>();
    private final Queue<Boolean> errorWindow = new LinkedList<>();
    private static final int WINDOW_SIZE = 20;

    private boolean isFirstProbe = true;

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

        if (isFirstProbe) {
            isFirstProbe = false;
            log.debug("Skipping first health check from latency window (connection warmup): {}ms, reachable={}",
                    latencyMs, reachable);
            return;
        }

        addToLatencyWindow(latencyMs);
        addToErrorWindow(!reachable);

        long sharedHealthySince = reachable
                ? updateAndReadSharedHealthySignal()
                : clearSharedHealthySignal();

        HealthCheckEvent event = new HealthCheckEvent(
                calculatePercentile(50),
                calculatePercentile(95),
                calculatePercentile(99),
                calculateErrorRate(),
                reachable,
                System.currentTimeMillis(),
                sharedHealthySince
        );

        log.debug("Health check: P50={}ms, P99={}ms, ErrorRate={}%, Reachable={}",
                event.p50LatencyMs(), event.p99LatencyMs(),
                String.format("%.1f", event.errorRate() * 100), reachable);

        var transition = stateMachine.evaluateHealth(event);
        if (transition != null) {
            alertSuppression.onStateTransition(transition);
        }
    }

    /**
     * Sets the shared "healthy since" timestamp if no instance has already
     * claimed one (so the earliest observer wins), or refreshes its TTL if
     * one already exists, keeping the original timestamp intact. Returns the
     * winning timestamp, or -1 if the write/read itself failed.
     */
    private long updateAndReadSharedHealthySignal() {
        try {
            String now = String.valueOf(System.currentTimeMillis());
            Boolean wasSet = redisTemplate.opsForValue().setIfAbsent(
                    HEALTHY_SINCE_KEY, now, Duration.ofSeconds(HEALTHY_SIGNAL_TTL_SECONDS));

            if (Boolean.TRUE.equals(wasSet)) {
                return Long.parseLong(now);
            }

            // Someone else already claimed it -- refresh TTL, keep their timestamp.
            redisTemplate.expire(HEALTHY_SINCE_KEY, Duration.ofSeconds(HEALTHY_SIGNAL_TTL_SECONDS));
            String existing = redisTemplate.opsForValue().get(HEALTHY_SINCE_KEY);
            return existing != null ? Long.parseLong(existing) : -1;
        } catch (Exception e) {
            log.debug("Failed to update shared healthy-since signal: {}", e.getMessage());
            return -1;
        }
    }

    /**
     * Best-effort clear of the shared signal when this instance sees Redis
     * as unreachable. If Redis is genuinely down this call will itself fail
     * silently -- that's fine, the key's TTL will expire it regardless.
     */
    private long clearSharedHealthySignal() {
        try {
            redisTemplate.delete(HEALTHY_SINCE_KEY);
        } catch (Exception e) {
            log.debug("Could not clear shared healthy-since signal (expected if Redis is down): {}", e.getMessage());
        }
        return -1;
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