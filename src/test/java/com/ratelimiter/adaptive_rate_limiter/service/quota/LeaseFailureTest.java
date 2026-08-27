package com.ratelimiter.adaptive_rate_limiter.service.quota;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.RateLimiterMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Phase 3C -- real Redis-death survive-then-degrade, end to end through Lettuce.
 *
 * <p>Unlike {@code LeaseManagerTest} (which simulates the outage with a fake
 * allocator flag), this drives an actual {@link GcraQuotaAllocator} against a
 * real Redis and then <b>stops the container mid-run</b>, proving that:
 * <ol>
 *   <li>a held, valid lease keeps admitting locally after Redis dies -- the fast
 *       path touches no Redis, so a genuine Lettuce connection failure never
 *       reaches the request; and</li>
 *   <li>once the held lease drains and Redis is still down, {@code tryAdmit}
 *       returns {@code null} -- the degrade signal -- rather than throwing a raw
 *       Redis error onto the hot path.</li>
 * </ol>
 *
 * <p>Uses a <b>dedicated</b> container it owns and can stop, never the shared
 * per-JVM fixture. A short Lettuce {@code commandTimeout} makes the lease against
 * a dead Redis fail fast (bounded), and a long lease TTL ensures the only reason
 * the bucket empties is real consumption -- not lease expiry -- so the degrade is
 * unambiguously attributable to exhaustion under outage.
 */
class LeaseFailureTest {

    private static final int LIMIT = 100;
    private static final int WINDOW_SECONDS = 60;
    private static final int K = 10; // round(0.1 * 100)

    private GenericContainer<?> redis;
    private LeaseManager manager;

    @BeforeEach
    void setUp() {
        redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379);
        redis.start();

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.setWindowSizeSeconds(WINDOW_SECONDS);
        RateLimiterProperties.Leasing leasing = properties.getLeasing();
        leasing.setEnabled(true);
        leasing.setMinLease(1);
        leasing.setMaxLeaseFraction(0.1);    // K = 10 at limit 100
        leasing.setPrefetchWatermark(0.0);   // no async prefetch: the only Redis hit is the sync lease
        leasing.setLeaseTtlMs(60_000);       // long TTL: bucket empties by consumption, not expiry

        GcraQuotaAllocator allocator = new GcraQuotaAllocator(
                templateFor(redis), new LuaScriptLoader(), properties);
        manager = new LeaseManager(allocator, properties, CircuitBreakerRegistry.ofDefaults(),
                new AdaptiveLeaseController(properties), new RateLimiterMetrics(new SimpleMeterRegistry()));
    }

    @AfterEach
    void tearDown() {
        if (redis != null && redis.isRunning()) {
            redis.stop();
        }
    }

    @Test
    void heldLeaseKeepsServingAfterRedisDies_thenDegradesOnExhaustion() {
        String key = "survive-real-redis";

        // Draw the first lease of K from a live Redis.
        RateLimitDecision first = manager.tryAdmit(key, LIMIT, WINDOW_SECONDS);
        assertNotNull(first, "first admit reaches a live Redis and draws a lease");
        assertTrue(first.allowed(), "first admit is served from the fresh lease");
        assertEquals(1, manager.syncLeaseCount(), "exactly one synchronous lease so far");

        // Redis dies mid-lease. Held tokens must keep serving with zero Redis contact.
        redis.stop();

        for (int i = 0; i < K - 1; i++) {
            RateLimitDecision d = manager.tryAdmit(key, LIMIT, WINDOW_SECONDS);
            assertNotNull(d, "held token " + i + " must serve while Redis is down (no raw error)");
            assertTrue(d.allowed(), "held token " + i + " must be admitted locally");
        }
        assertEquals(K - 1, manager.localAdmitCount(), "the K-1 held admits touched no Redis");
        assertEquals(1, manager.syncLeaseCount(), "still just the one pre-outage lease");

        // Lease drained + Redis still down -> degrade signal, not an exception.
        assertNull(manager.tryAdmit(key, LIMIT, WINDOW_SECONDS),
                "drained lease with Redis down must signal degrade (null)");
    }

    @Test
    void coldBucketWithRedisDownSignalsDegradeNotError() {
        redis.stop(); // down before any lease is ever drawn

        RateLimitDecision d = manager.tryAdmit("cold-real-redis", LIMIT, WINDOW_SECONDS);

        assertNull(d, "empty bucket + unreachable Redis must degrade (null), never propagate a Redis error");
    }

    /**
     * A template pointed at the given container with a short command timeout, so a
     * command issued after the container stops fails fast (bounded) instead of
     * blocking the test -- the LeaseManager then swallows it into a degrade signal.
     */
    private static RedisTemplate<String, String> templateFor(GenericContainer<?> container) {
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(500))
                .shutdownTimeout(Duration.ofMillis(100))
                .build();
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(container.getHost(), container.getMappedPort(6379)),
                clientConfig);
        connectionFactory.afterPropertiesSet();

        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
}
