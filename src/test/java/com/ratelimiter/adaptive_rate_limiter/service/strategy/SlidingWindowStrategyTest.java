package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * Baseline correctness tests for the existing, UNMODIFIED
 * {@link SlidingWindowStrategy}, run under equivalent conditions to
 * {@link GcraStrategyTest} so the two algorithms can be fairly compared.
 * This file adds test coverage only; the strategy implementation itself is
 * untouched.
 */
class SlidingWindowStrategyTest extends RedisStrategyTestBase {

    @Test
    void firstRequestForNewKeyIsAllowed() {
        String key = uniqueKey("sw-basic");

        RateLimitDecision decision = slidingWindowStrategy.isAllowed(key, 10, 1);

        assertTrue(decision.allowed());
        assertEquals("SLIDING_WINDOW", decision.algorithmUsed());
        assertEquals(9, decision.remaining());
    }

    @Test
    void burstOfExactlyLimitRequestsIsAllowedInstantly() {
        String key = uniqueKey("sw-burst");
        int limit = 20;

        for (int i = 0; i < limit; i++) {
            RateLimitDecision decision = slidingWindowStrategy.isAllowed(key, limit, 1);
            assertTrue(decision.allowed(), "request " + i + " of the burst should be allowed");
        }

        RateLimitDecision overLimit = slidingWindowStrategy.isAllowed(key, limit, 1);
        assertFalse(overLimit.allowed(), "the (limit+1)-th instantaneous request must be rejected");
    }

    @Test
    void requestsExceedingRateAreEventuallyRejected() {
        String key = uniqueKey("sw-rate");
        int limit = 5;

        int allowed = 0;
        int denied = 0;
        for (int i = 0; i < limit * 3; i++) {
            RateLimitDecision decision = slidingWindowStrategy.isAllowed(key, limit, 1);
            if (decision.allowed()) {
                allowed++;
            } else {
                denied++;
            }
        }

        assertEquals(limit, allowed);
        assertTrue(denied > 0);
    }

    @Test
    void trafficForOneKeyDoesNotAffectAnotherKey() {
        String keyA = uniqueKey("sw-a");
        String keyB = uniqueKey("sw-b");
        int limit = 3;

        for (int i = 0; i < limit; i++) {
            assertTrue(slidingWindowStrategy.isAllowed(keyA, limit, 10).allowed());
        }
        assertFalse(slidingWindowStrategy.isAllowed(keyA, limit, 10).allowed());

        for (int i = 0; i < limit; i++) {
            assertTrue(slidingWindowStrategy.isAllowed(keyB, limit, 10).allowed(),
                    "key B must not be affected by key A's traffic");
        }
    }

    @Test
    void inactiveKeyExpires() throws InterruptedException {
        String key = uniqueKey("sw-expire");
        int limit = 2;
        int windowSeconds = 1;

        assertTrue(slidingWindowStrategy.isAllowed(key, limit, windowSeconds).allowed());
        assertTrue(slidingWindowStrategy.isAllowed(key, limit, windowSeconds).allowed());
        assertFalse(slidingWindowStrategy.isAllowed(key, limit, windowSeconds).allowed());

        String redisKey = "ratelimit:" + key + ":window";
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)));

        Thread.sleep((windowSeconds * 1000L) + 1500);

        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)),
                "Sliding Window key must expire once all entries have aged out, bounding Redis memory");
    }

    @Test
    void concurrentRequestsForTheSameKeyNeverExceedTheLimit() throws InterruptedException {
        // Same real-Redis, real-Lua concurrency exercise as
        // GcraStrategyTest's equivalent, using a window generous enough
        // (5s) that no entry ages out mid-test, so Sliding Window's bound
        // is a hard, exact cap for the whole run.
        String key = uniqueKey("sw-concurrency");
        int limit = 50;
        int threadCount = 200;

        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger allowedCount = new AtomicInteger();

        for (int i = 0; i < threadCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    if (slidingWindowStrategy.isAllowed(key, limit, 5).allowed()) {
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
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(limit, allowedCount.get(),
                "exactly `limit` of the simultaneous requests should be admitted -- Redis atomicity must "
                        + "prevent over-admission even under real concurrency");
    }
}