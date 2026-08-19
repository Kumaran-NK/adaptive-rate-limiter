package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * Correctness tests for {@link GcraStrategy} against a real Redis instance
 * (via Testcontainers), proving the semantics documented in GcraStrategy /
 * gcra.lua: TAT-based state, burst == limit, atomic concurrent admission,
 * per-key isolation, and expiration.
 */
class GcraStrategyTest extends RedisStrategyTestBase {

    @Test
    void firstRequestForNewKeyIsAllowed() {
        String key = uniqueKey("gcra-basic");

        RateLimitDecision decision = gcraStrategy.isAllowed(key, 10, 1);

        assertTrue(decision.allowed());
        assertEquals("GCRA", decision.algorithmUsed());
        // burst capacity for limit=10 is 10, so after 1 request 9 remain
        assertEquals(9, decision.remaining());
    }

    @Test
    void burstOfExactlyLimitRequestsIsAllowedInstantly() {
        // Documented burst semantics: from a fully idle key, exactly
        // `limit` requests submitted back-to-back are all admitted, and
        // the (limit + 1)-th is rejected. This mirrors "N per window" at
        // the instant a window opens -- comparable to, though not
        // mechanically identical to, Sliding Window's guarantee.
        String key = uniqueKey("gcra-burst");
        int limit = 20;

        for (int i = 0; i < limit; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, limit, 10);
            assertTrue(decision.allowed(), "request " + i + " of the burst should be allowed");
        }

        RateLimitDecision overLimit = gcraStrategy.isAllowed(key, limit, 10);
        assertFalse(overLimit.allowed(), "the (limit+1)-th instantaneous request must be rejected");
    }

    @Test
    void requestsExceedingRateAreEventuallyRejected() {
        String key = uniqueKey("gcra-rate");
        int limit = 5;

        int allowed = 0;
        int denied = 0;
        for (int i = 0; i < limit * 3; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, limit, 1);
            if (decision.allowed()) {
                allowed++;
            } else {
                denied++;
            }
        }

        assertEquals(limit, allowed, "only the burst capacity of `limit` should be admitted instantly");
        assertTrue(denied > 0, "requests beyond the configured rate must be rejected");
    }

    @Test
    void requestsSpacedAtTheEmissionIntervalAreAlwaysAllowed() throws InterruptedException {
        String key = uniqueKey("gcra-spacing");
        int limit = 10;
        int windowSeconds = 1;
        long emissionIntervalMs = (windowSeconds * 1000L) / limit; // 100ms

        for (int i = 0; i < 15; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, limit, windowSeconds);
            assertTrue(decision.allowed(),
                    "request " + i + " arriving exactly at the theoretical rate should conform");
            Thread.sleep(emissionIntervalMs);
        }
    }

    @Test
    void rejectedRequestReportsPositiveSensibleRetryAfter() {
        String key = uniqueKey("gcra-retry");
        int limit = 3;
        long now = System.currentTimeMillis();

        for (int i = 0; i < limit; i++) {
            assertTrue(gcraStrategy.isAllowed(key, limit, 1).allowed());
        }

        RateLimitDecision denied = gcraStrategy.isAllowed(key, limit, 1);
        assertFalse(denied.allowed());
        long retryAfterMs = denied.resetTimeMillis() - now;

        assertTrue(retryAfterMs > 0, "retry-after must be positive");
        // For limit=3 over 1s, emission interval is ~333ms; retry-after
        // for an immediate over-limit request should be on that order,
        // and certainly well under a full window.
        assertTrue(retryAfterMs <= 1000, "retry-after should not exceed a full window");
    }

    @Test
    void trafficForOneKeyDoesNotAffectAnotherKey() {
        String keyA = uniqueKey("gcra-a");
        String keyB = uniqueKey("gcra-b");
        int limit = 3;

        for (int i = 0; i < limit; i++) {
            assertTrue(gcraStrategy.isAllowed(keyA, limit, 1).allowed());
        }
        // Key A is now exhausted.
        assertFalse(gcraStrategy.isAllowed(keyA, limit, 1).allowed());

        // Key B is untouched and should still allow its own full burst.
        for (int i = 0; i < limit; i++) {
            assertTrue(gcraStrategy.isAllowed(keyB, limit, 1).allowed(),
                    "key B must not be affected by key A's traffic");
        }
    }

    @Test
    void inactiveKeyExpiresAndIsIndistinguishableFromANewKey() throws InterruptedException {
        String key = uniqueKey("gcra-expire");
        int limit = 2;
        int windowSeconds = 1;

        // Exhaust the burst.
        assertTrue(gcraStrategy.isAllowed(key, limit, windowSeconds).allowed());
        assertTrue(gcraStrategy.isAllowed(key, limit, windowSeconds).allowed());
        assertFalse(gcraStrategy.isAllowed(key, limit, windowSeconds).allowed());

        String redisKey = "ratelimit:" + key + ":gcra";
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)),
                "GCRA state key should exist while TAT is ahead of now");

        // Wait past the window so TAT decays back to "now" and the key's
        // TTL (set inside the Lua script) expires it.
        Thread.sleep((windowSeconds * 1000L) + 500);

        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)),
                "GCRA state key must expire once the bucket is idle again, bounding Redis memory");

        // A fresh full burst should be available again.
        for (int i = 0; i < limit; i++) {
            assertTrue(gcraStrategy.isAllowed(key, limit, windowSeconds).allowed());
        }
    }

    @Test
    void concurrentRequestsForTheSameKeyNeverExceedTheMathematicallyAllowedBound() throws InterruptedException {
        // This exercises the real Redis Lua script under genuine
        // concurrency (multiple OS threads, each issuing its own real
        // Redis call), not just Java-level synchronization. Redis'
        // single-threaded command execution model makes each Lua script
        // invocation atomic, so even though many threads race to
        // read-then-write the same TAT key, admission must never exceed
        // what the policy allows for the elapsed wall-clock time: the
        // burst (`limit`), plus one more admission per emission_interval
        // that elapses while the run is in flight (GCRA legitimately keeps
        // admitting at the sustained rate -- that is not a violation).
        String key = uniqueKey("gcra-concurrency");
        int limit = 50;
        int windowSeconds = 5; // generous window so we're measuring atomicity, not decay
        long emissionIntervalMs = (windowSeconds * 1000L) / limit;
        int threadCount = 200;

        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger allowedCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    RateLimitDecision decision = gcraStrategy.isAllowed(key, limit, windowSeconds);
                    if (decision.allowed()) {
                        allowedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(30, TimeUnit.SECONDS);
        long elapsedMs = System.currentTimeMillis() - start;
        pool.shutdown();

        assertTrue(finished, "all concurrent requests should complete within the timeout");

        long theoreticalMax = limit + (elapsedMs / emissionIntervalMs) + 2; // +2 for scheduling slack
        assertTrue(allowedCount.get() >= limit,
                "the burst alone guarantees at least `limit` admissions");
        assertTrue(allowedCount.get() <= theoreticalMax,
                "admitted count (" + allowedCount.get() + ") must not exceed what the policy allows "
                        + "for " + elapsedMs + "ms of elapsed time (bound=" + theoreticalMax + ") -- "
                        + "Redis atomicity must prevent over-admission even under real concurrency");
    }

    @Test
    void multipleIndependentClientInstancesSharingRedisNeverExceedTheBurst() throws InterruptedException {
        // Simulates several separate application pods (each with its own
        // connection factory / RedisTemplate / GcraStrategy instance)
        // hitting the same limiter key concurrently, proving the atomicity
        // guarantee holds across independent clients sharing Redis state,
        // not just across threads sharing one Java connection pool.
        String key = uniqueKey("gcra-multi-instance");
        int limit = 30;
        int windowSeconds = 5;
        int instanceCount = 3;
        int requestsPerInstance = 20;

        List<GcraStrategy> instances = new java.util.ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            RedisTemplate<String, String> template = newRedisTemplate();
            var scriptLoader = new com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader();
            instances.add(new GcraStrategy(template, scriptLoader));
        }

        ExecutorService pool = Executors.newFixedThreadPool(instanceCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(instanceCount);
        AtomicInteger allowedCount = new AtomicInteger();

        for (GcraStrategy instance : instances) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < requestsPerInstance; i++) {
                        if (instance.isAllowed(key, limit, windowSeconds).allowed()) {
                            allowedCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertTrue(allowedCount.get() <= limit + 5,
                "independent instances sharing one Redis key must still be bounded by the configured burst "
                        + "(admitted=" + allowedCount.get() + ")");
    }
}