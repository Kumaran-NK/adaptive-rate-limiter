package com.ratelimiter.adaptive_rate_limiter.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.RateLimitStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * Controlled, non-production benchmark comparing {@code SlidingWindowStrategy}
 * against {@code GcraStrategy} under identical workloads, against the same
 * real Redis instance.
 *
 * <p>This class deliberately does NOT follow the {@code *Test.java} naming
 * pattern Surefire auto-discovers, so it will not run as part of a normal
 * {@code mvn test}. Run it explicitly -- see the README section on running
 * the experiment for exact commands.
 *
 * <p>Every number reported here is measured at run time against the actual
 * strategies; nothing is invented or assumed.
 */
class RateLimiterAlgorithmBenchmark extends RedisStrategyTestBase {

    private static final int LIMIT = 100;
    private static final int WINDOW_SECONDS = 1;

    @Test
    void scenarioA_lowTraffic() {
        // Well under the limit: a handful of requests per key, low
        // concurrency.
        runScenario("A - LOW TRAFFIC", 1, 20, 2, LIMIT, WINDOW_SECONDS);
    }

    @Test
    void scenarioB_atLimit() {
        // Traffic approximately equal to the configured limit.
        runScenario("B - AT LIMIT", 1, LIMIT, 4, LIMIT, WINDOW_SECONDS);
    }

    @Test
    void scenarioC_highTraffic() {
        // Traffic significantly exceeds the configured limit.
        runScenario("C - HIGH TRAFFIC", 1, LIMIT * 5, 16, LIMIT, WINDOW_SECONDS);
    }

    @Test
    void scenarioD_burstThenSustained() throws InterruptedException {
        System.out.println("\n=== SCENARIO D - BURST THEN SUSTAINED ===");
        for (RateLimitStrategy strategy : List.of(slidingWindowStrategy, gcraStrategy)) {
            redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
            String key = uniqueKey("bench-burst-sustained-" + strategy.getAlgorithmName());

            BenchmarkResult burst = execute(strategy, List.of(key), LIMIT * 2, 8, LIMIT, WINDOW_SECONDS);
            Thread.sleep(WINDOW_SECONDS * 1000L + 100);
            BenchmarkResult sustained = execute(strategy, List.of(key), LIMIT, 1, LIMIT, WINDOW_SECONDS);

            System.out.printf("%-14s burst-phase: %s%n", strategy.getAlgorithmName(), burst);
            System.out.printf("%-14s sustained-phase (after 1 window): %s%n", strategy.getAlgorithmName(), sustained);
        }
    }

    @Test
    void scenarioE_concurrentSameKey() {
        // Many concurrent clients hitting the SAME key.
        runScenario("E - CONCURRENT SAME KEY", 1, 500, 64, LIMIT, WINDOW_SECONDS);
    }

    @Test
    void scenarioF_manyKeys() {
        // Many independent rate-limit keys, moderate traffic each.
        runScenario("F - MANY KEYS", 200, 5, 16, LIMIT, WINDOW_SECONDS);
    }

    // ---------------------------------------------------------------
    // Shared harness
    // ---------------------------------------------------------------

    private void runScenario(String name, int numKeys, int requestsPerKey, int concurrency,
                              int limit, int windowSeconds) {
        System.out.println("\n=== SCENARIO " + name + " ===");
        System.out.printf("keys=%d requestsPerKey=%d concurrency=%d limit=%d windowSeconds=%d%n",
                numKeys, requestsPerKey, concurrency, limit, windowSeconds);

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            keys.add(uniqueKey(name.replaceAll("\\s+", "-") + "-" + i));
        }

        System.out.printf("%-14s %s%n", "Algorithm", "Result");
        for (RateLimitStrategy strategy : List.of(slidingWindowStrategy, gcraStrategy)) {
            redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
            BenchmarkResult result = execute(strategy, keys, requestsPerKey, concurrency, limit, windowSeconds);
            RedisStateReport stateReport = reportRedisState(strategy.getAlgorithmName());
            System.out.printf("%-14s %s | %s%n", strategy.getAlgorithmName(), result, stateReport);
        }
    }

    private BenchmarkResult execute(RateLimitStrategy strategy, List<String> keys, int requestsPerKey,
                                     int concurrency, int limit, int windowSeconds) {
        int totalRequests = keys.size() * requestsPerKey;
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, concurrency));
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalRequests);
        AtomicInteger allowed = new AtomicInteger();
        AtomicInteger denied = new AtomicInteger();
        long[] latenciesNanos = new long[totalRequests];
        AtomicInteger latencyIndex = new AtomicInteger();

        long wallStart = System.nanoTime();
        for (String key : keys) {
            for (int i = 0; i < requestsPerKey; i++) {
                pool.submit(() -> {
                    try {
                        startLatch.await();
                        long callStart = System.nanoTime();
                        RateLimitDecision decision = strategy.isAllowed(key, limit, windowSeconds);
                        long callElapsed = System.nanoTime() - callStart;
                        latenciesNanos[latencyIndex.getAndIncrement()] = callElapsed;
                        if (decision.allowed()) {
                            allowed.incrementAndGet();
                        } else {
                            denied.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
        }

        startLatch.countDown();
        try {
            doneLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long wallElapsedNanos = System.nanoTime() - wallStart;
        pool.shutdown();

        int actualCount = latencyIndex.get();
        long[] usedLatencies = Arrays.copyOf(latenciesNanos, actualCount);
        Arrays.sort(usedLatencies);

        double throughputPerSec = actualCount / (wallElapsedNanos / 1_000_000_000.0);

        return new BenchmarkResult(
                actualCount,
                allowed.get(),
                denied.get(),
                throughputPerSec,
                toMillis(average(usedLatencies)),
                toMillis(percentile(usedLatencies, 50)),
                toMillis(percentile(usedLatencies, 95)),
                toMillis(percentile(usedLatencies, 99))
        );
    }

    private RedisStateReport reportRedisState(String algorithmLabel) {
        Set<String> keys = redisTemplate.keys("ratelimit:*");
        int keyCount = keys == null ? 0 : keys.size();

        long totalZsetEntries = 0;
        if (keys != null) {
            for (String k : keys) {
                if (k.endsWith(":window")) {
                    Long size = redisTemplate.opsForZSet().size(k);
                    totalZsetEntries += size != null ? size : 0;
                }
            }
        }

        return new RedisStateReport(keyCount, totalZsetEntries);
    }

    private static double average(long[] values) {
        if (values.length == 0) return 0;
        long sum = 0;
        for (long v : values) sum += v;
        return (double) sum / values.length;
    }

    private static double percentile(long[] sortedValues, double p) {
        if (sortedValues.length == 0) return 0;
        int index = (int) Math.ceil(p / 100.0 * sortedValues.length) - 1;
        index = Math.max(0, Math.min(index, sortedValues.length - 1));
        return sortedValues[index];
    }

    private static double toMillis(double nanos) {
        return nanos / 1_000_000.0;
    }

    private record BenchmarkResult(
            int totalRequests,
            int allowed,
            int denied,
            double throughputPerSec,
            double avgLatencyMs,
            double p50LatencyMs,
            double p95LatencyMs,
            double p99LatencyMs
    ) {
        @Override
        public String toString() {
            return String.format(
                    "total=%d allowed=%d denied=%d throughput=%.1f/s avg=%.2fms p50=%.2fms p95=%.2fms p99=%.2fms",
                    totalRequests, allowed, denied, throughputPerSec, avgLatencyMs, p50LatencyMs, p95LatencyMs, p99LatencyMs);
        }
    }

    private record RedisStateReport(int keyCount, long totalSortedSetEntries) {
        @Override
        public String toString() {
            return String.format("redisKeys=%d slidingWindowZsetEntries=%d (GCRA stores 1 scalar per key, 0 zset entries)",
                    keyCount, totalSortedSetEntries);
        }
    }
}