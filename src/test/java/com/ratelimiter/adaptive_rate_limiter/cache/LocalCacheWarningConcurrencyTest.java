package com.ratelimiter.adaptive_rate_limiter.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Caffeine;

class LocalCacheWarningConcurrencyTest {

    @Test
    void tryConsumeShouldEliminateLostUpdatesUnderHighConcurrency() throws InterruptedException {
        LocalRateLimitCache localCache = new LocalRateLimitCache(
                Caffeine.newBuilder().build()
        );

        String key = "test-endpoint-key";
        int initialTokens = 100;
        localCache.put(key, initialTokens);

        int totalThreads = 120; // 100 should be allowed, 20 denied
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);

        AtomicInteger allowedCount = new AtomicInteger();
        AtomicInteger deniedCount = new AtomicInteger();

        for (int i = 0; i < totalThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    LocalRateLimitCache.ConsumeResult result = localCache.tryConsume(key);
                    if (result.allowed()) {
                        allowedCount.incrementAndGet();
                    } else {
                        deniedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Fire all threads concurrently
        doneLatch.await();
        executor.shutdown();

        assertEquals(100, allowedCount.get(), "Exactly 100 requests must be allowed");
        assertEquals(20, deniedCount.get(), "Exactly 20 requests must be denied");
        assertEquals(0, localCache.get(key), "Remaining tokens in cache must be exactly 0");

        // Additional consumption attempt must return allowed=false
        LocalRateLimitCache.ConsumeResult extraResult = localCache.tryConsume(key);
        assertFalse(extraResult.allowed(), "Requests past capacity must be denied");
    }
}
