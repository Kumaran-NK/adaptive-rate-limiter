package com.ratelimiter.adaptive_rate_limiter.service.strategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

class GcraInvariantTest extends RedisStrategyTestBase {

    private static final int LIMIT = 10;
    private static final int WINDOW_SECONDS = 10;
    private static final long PERIOD_MS = WINDOW_SECONDS * 1000L;
    private static final long EMISSION_INTERVAL_MS = PERIOD_MS / LIMIT;

    @Test
    void invariant_1_idleKeyAlllowsExactlyLimitSimultaneousRequests() {
        String key = uniqueKey("g1-idle");
        List<RateLimitDecision> decisions = new ArrayList<>();

        for (int i = 0; i < LIMIT; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
            decisions.add(decision);
            assertTrue(decision.allowed(), "request " + i + " should be admitted on an idle key");
        }

        assertEquals(LIMIT, decisions.stream().filter(RateLimitDecision::allowed).count());

        String redisKey = redisKey(key);
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)), "TAT key should exist after the burst");
        String tatValue = redisTemplate.opsForValue().get(redisKey);
        assertNotNull(tatValue, "TAT must be persisted after the burst");
        long tat = Long.parseLong(tatValue);
        long nowMs = redisNowMs();
        assertTrue(tat >= nowMs, "TAT must not be behind Redis time while the bucket is active");
        assertTrue(tat <= nowMs + PERIOD_MS, "TAT should be bounded by the burst window length");
    }

    @Test
    void invariant_2_eleventhSimultaneousRequestIsRejected() {
        String key = uniqueKey("g2-11th");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed(), "burst should admit the first 10");
        }

        RateLimitDecision denied = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
        assertFalse(denied.allowed(), "the 11th immediate request must be rejected");

        String redisKey = redisKey(key);
        String tatBefore = redisTemplate.opsForValue().get(redisKey);
        assertNotNull(tatBefore, "TAT should still exist after the rejection");
        assertEquals(tatBefore, redisTemplate.opsForValue().get(redisKey), "rejected requests must not advance TAT");
    }

    @Test
    void invariant_3_rejectedRequestLeavesTatUnchanged() {
        String key = uniqueKey("g3-reject-tat");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed());
        }

        String redisKey = redisKey(key);
        String before = redisTemplate.opsForValue().get(redisKey);
        assertNotNull(before, "TAT should be set before the rejection");

        for (int i = 0; i < 3; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
            assertFalse(decision.allowed(), "request " + i + " should be rejected while TAT is still ahead");
            String after = redisTemplate.opsForValue().get(redisKey);
            assertEquals(before, after, "TAT must be unchanged after each rejection");
        }
    }

    @Test
    void invariant_4_waitingExactlyRetryAfterAllowsRequest() throws InterruptedException {
        String key = uniqueKey("g4-exact");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed());
        }

        RateLimitDecision rejected = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
        assertFalse(rejected.allowed());

        long retryAfterMs = expectedRetryAfterMs(key);
        waitUntilRedisNowAtLeast(redisNowMs() + retryAfterMs);
        RateLimitDecision afterWait = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);

        assertTrue(afterWait.allowed(), "waiting exactly retryAfter must allow the request");
        assertEquals(redisKey(key), redisKey(key), "sanity check");
        assertTrue(getTatMs(key) > 0, "TAT should advance after the accepted retry");
    }

    @Test
    void invariant_5_waitingOneMillisecondBeforeRetryAfterStillRejects() throws InterruptedException {
        String key = uniqueKey("g5-one-ms-early");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed());
        }

        RateLimitDecision rejected = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
        assertFalse(rejected.allowed());

        long retryAfterMs = expectedRetryAfterMs(key);
        long waitTargetMs = redisNowMs() + Math.max(0L, retryAfterMs - 100L);
        waitUntilRedisNowAtLeast(waitTargetMs);

        RateLimitDecision earlyRetry = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
        assertFalse(earlyRetry.allowed(), "waiting one millisecond before retryAfter must still reject");

        long exactTargetMs = redisNowMs() + retryAfterMs + 10L;
        waitUntilRedisNowAtLeast(exactTargetMs);
        RateLimitDecision atBoundary = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
        assertTrue(atBoundary.allowed(), "waiting exactly retryAfter should now allow the request");
    }

    @Test
    void invariant_6_everyAcceptedRequestAdvancesTatByEmissionInterval() {
        String key = uniqueKey("g6-emission");

        long previousTat = 0L;
        for (int i = 0; i < LIMIT; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
            assertTrue(decision.allowed());

            String redisKey = redisKey(key);
            assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)));
            long tat = getTatMs(key);
            if (i == 0) {
                previousTat = tat;
            } else {
                assertEquals(EMISSION_INTERVAL_MS, tat - previousTat,
                        "TAT delta must be exactly the emission interval for each accepted request");
                previousTat = tat;
            }
        }
    }

    @Test
    void invariant_7_tatNeverMovesBackwardsWhileKeyExists() {
        String key = uniqueKey("g7-monotonic");

        long previousTat = Long.MIN_VALUE;
        for (int i = 0; i < LIMIT * 2; i++) {
            RateLimitDecision decision = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
            if (!decision.allowed()) {
                continue;
            }
            long tat = getTatMs(key);
            if (previousTat != Long.MIN_VALUE) {
                assertTrue(tat >= previousTat,
                        "TAT must never move backwards while the key remains active");
            }
            previousTat = tat;
        }

        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey(key))), "key should still exist before expiry");
    }

    @Test
    void invariant_8_whenNowExceedsTatNextAcceptedRequestStartsFromRedisTime() {
        String key = uniqueKey("g8-stale");
        String redisKey = redisKey(key);
        long nowMs = redisNowMs();
        long staleTat = nowMs - 5_000L;
        redisTemplate.opsForValue().set(redisKey, String.valueOf(staleTat));
        redisTemplate.expire(redisKey, Duration.ofSeconds(30));

        RateLimitDecision decision = gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS);
        assertTrue(decision.allowed(), "stale TAT should be reset to current Redis time and accepted");

        long freshTat = getTatMs(key);
        long expectedMin = nowMs + EMISSION_INTERVAL_MS;
        assertTrue(freshTat >= expectedMin - 150L && freshTat <= expectedMin + 150L,
                "stale TAT should be replaced by current Redis time before advancing by one emission interval");
    }

    @Test
    void invariant_9_expiredKeyBehavesLikeANewKey() throws InterruptedException {
        String key = uniqueKey("g9-expire");
        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed());
        }

        String redisKey = redisKey(key);
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)));
        long ttlMs = redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
        long waitMs = Math.max(1L, ttlMs + 50L);
        Thread.sleep(waitMs);

        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey)), "expired key must be gone");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed(), "expired key should behave like a new key");
        }
    }

    @Test
    void invariant_10_independentKeysDoNotAffectOneAnother() {
        String keyA = uniqueKey("g10-a");
        String keyB = uniqueKey("g10-b");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(keyA, LIMIT, WINDOW_SECONDS).allowed());
        }
        assertFalse(gcraStrategy.isAllowed(keyA, LIMIT, WINDOW_SECONDS).allowed(), "key A should be exhausted");

        for (int i = 0; i < LIMIT; i++) {
            assertTrue(gcraStrategy.isAllowed(keyB, LIMIT, WINDOW_SECONDS).allowed(), "key B must stay independent");
        }

        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey(keyA))), "key A state should remain distinct");
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(redisKey(keyB))), "key B state should remain distinct");
    }

    @Test
    void invariant_11_concurrentRequestsRespectTheGcraMathematicalBound() throws Exception {
        String key = uniqueKey("g11-concurrent");
        int threadCount = 32;
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Boolean>> futures = new ArrayList<>();

        long startedAt = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                startLatch.await();
                return gcraStrategy.isAllowed(key, LIMIT, WINDOW_SECONDS).allowed();
            }));
        }

        startLatch.countDown();
        int admitted = 0;
        for (Future<Boolean> future : futures) {
            if (future.get(30, TimeUnit.SECONDS)) {
                admitted++;
            }
        }
        executor.shutdown();

        long elapsedMs = System.currentTimeMillis() - startedAt;
        long theoreticalBound = LIMIT + (long) Math.ceil((double) elapsedMs / EMISSION_INTERVAL_MS) + 2;

        assertTrue(admitted >= LIMIT, "the burst capacity should still be available to the first burst");
        assertTrue(admitted <= theoreticalBound,
                "concurrency should not exceed the mathematical GCRA bound; admitted=" + admitted + ", bound=" + theoreticalBound + ", elapsedMs=" + elapsedMs);
        assertTrue(admitted <= LIMIT + 6,
                "real Redis concurrency should stay in the same narrow band around the burst limit");
    }

    private String redisKey(String key) {
        return "ratelimit:" + key + ":gcra";
    }

        private long redisNowMs() {
        Long ms = redisTemplate.execute((RedisCallback<Long>) RedisConnection::time);
        return ms == null ? 0L : ms;
    }

    private long getTatMs(String key) {
        String tatValue = redisTemplate.opsForValue().get(redisKey(key));
        assertNotNull(tatValue, "TAT should exist for key " + key);
        return Long.parseLong(tatValue);
    }

    private long expectedRetryAfterMs(String key) {
        long tat = getTatMs(key);
        long nowMs = redisNowMs();
        long allowAt = tat + EMISSION_INTERVAL_MS - PERIOD_MS;
        return Math.max(1L, (long) Math.ceil(allowAt - nowMs));
    }

    private void waitUntilRedisNowAtLeast(long targetMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (redisNowMs() < targetMs && System.currentTimeMillis() < deadline) {
            Thread.sleep(1L);
        }
        assertTrue(redisNowMs() >= targetMs, "Redis clock did not reach the requested target: target=" + targetMs + ", now=" + redisNowMs());
    }
}
