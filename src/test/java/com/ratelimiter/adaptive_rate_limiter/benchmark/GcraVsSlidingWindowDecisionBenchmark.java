package com.ratelimiter.adaptive_rate_limiter.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.GcraStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.RateLimitStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

/**
 * <b>Decision-grade</b> comparison of {@link SlidingWindowStrategy} vs
 * {@link GcraStrategy}, intended to answer one question with data:
 * <em>should GCRA replace Sliding Window as the default distributed
 * strategy?</em>
 *
 * <p>This supersedes {@code RateLimiterAlgorithmBenchmark} (which remains as a
 * quick directional smoke test). The differences that make this one
 * decision-grade rather than directional:
 * <ul>
 *   <li><b>Warm-up.</b> Both algorithms are exercised hard before any
 *       measurement, so neither pays the JIT / Lettuce-connection /
 *       Lua-script-cache cold-start tax while being timed. (The directional
 *       benchmark always ran Sliding Window first, systematically penalising
 *       it.)</li>
 *   <li><b>Per-worker Redis connections.</b> Each worker thread gets its own
 *       {@link LettuceConnectionFactory}. A single shared multiplexed
 *       connection would cap <em>both</em> algorithms at the same client-side
 *       ceiling and mask the server-side cost difference we are trying to
 *       measure.</li>
 *   <li><b>Sustained closed-loop load + repeated trials.</b> Fixed-duration
 *       load yields a real throughput number and a full latency distribution
 *       (p50/p95/p99/p999). Every scenario is run over several trials and the
 *       median is reported with min/max, so a single GC pause cannot decide an
 *       architecture.</li>
 *   <li><b>Realistic workloads.</b> Scenarios mirror the project's actual
 *       endpoint configuration (payment 10/60s, search 200/60s) and include
 *       the case a rate limiter exists for: a hot key under sustained
 *       over-limit abuse, where the cost of the <em>deny</em> path dominates.
 *       A footprint measurement compares Redis memory per key.</li>
 * </ul>
 *
 * <p><b>Validity note on absolute numbers.</b> This runs against a local
 * Testcontainers Redis, so absolute latencies do not include production
 * network RTT. That does <em>not</em> distort the comparison: both strategies
 * issue exactly one Lua {@code EVAL} per decision (one round trip), so network
 * RTT is an identical constant added to both. The measured <em>delta</em>
 * between GCRA and Sliding Window is therefore RTT-independent and reflects
 * Redis server-side work plus Java serialization -- exactly the component that
 * differs between the two and that generalises to production. What this does
 * NOT model: multiple Redis nodes and multiple JVMs (concurrency here is
 * threads within one JVM against one Redis).
 *
 * <p>Not named {@code *Test}, so it is skipped by a normal {@code mvn test}.
 * Run explicitly, e.g.:
 * <pre>
 *   sh mvnw -Dtest=GcraVsSlidingWindowDecisionBenchmark -DfailIfNoTests=false \
 *           -Dsurefire.useFile=false test
 * </pre>
 * Tunable via {@code -Dbench.trials}, {@code -Dbench.durationMs},
 * {@code -Dbench.warmupMs}, {@code -Dbench.concurrency},
 * {@code -Dbench.footprintKeys}.
 */
class GcraVsSlidingWindowDecisionBenchmark extends RedisStrategyTestBase {

    private static final int TRIALS = intProp("bench.trials", 5);
    private static final long DURATION_MS = longProp("bench.durationMs", 3000);
    private static final long WARMUP_MS = longProp("bench.warmupMs", 2000);
    private static final int CONCURRENCY = intProp("bench.concurrency", 16);
    private static final int FOOTPRINT_KEYS = intProp("bench.footprintKeys", 1000);

    /** How each scenario draws the key(s) each request targets. */
    private enum KeyMode {
        /** All workers hammer ONE shared key (models a hot key / abusive client). */
        HOT_KEY,
        /** Workers rotate over a large key space (models many distinct clients). */
        MANY_KEYS
    }

    private record Scenario(String name, int limit, int windowSeconds, KeyMode keyMode) {}

    private static final List<Scenario> SCENARIOS = List.of(
            new Scenario("Many clients, healthy (payment 10/60s)", 10, 60, KeyMode.MANY_KEYS),
            new Scenario("Many clients, healthy (search 200/60s)", 200, 60, KeyMode.MANY_KEYS),
            new Scenario("Hot key under abuse (search 200/60s)", 200, 60, KeyMode.HOT_KEY),
            new Scenario("Hot key, high limit (synthetic 5000/60s)", 5000, 60, KeyMode.HOT_KEY)
    );

    @Test
    void runDecisionBenchmark() {
        StringBuilder report = new StringBuilder();
        report.append("\n===BENCH-REPORT-START===\n");
        report.append("# GCRA vs Sliding Window -- decision-grade benchmark\n\n");
        report.append(String.format(
                "config: trials=%d durationMs=%d warmupMs=%d concurrency=%d footprintKeys=%d%n",
                TRIALS, DURATION_MS, WARMUP_MS, CONCURRENCY, FOOTPRINT_KEYS));
        report.append("redis: local Testcontainers redis:7-alpine (see class Javadoc re: absolute vs delta)\n\n");

        LuaScriptLoader scriptLoader = new LuaScriptLoader();

        // One dedicated connection (and one strategy instance bound to it) per
        // worker thread, built once and reused across every scenario/trial.
        List<LettuceConnectionFactory> factories = new ArrayList<>();
        List<RateLimitStrategy> sw = new ArrayList<>();
        List<RateLimitStrategy> gcra = new ArrayList<>();
        for (int i = 0; i < CONCURRENCY; i++) {
            LettuceConnectionFactory factory = newFactory();
            factories.add(factory);
            RedisTemplate<String, String> template = templateFor(factory);
            sw.add(new SlidingWindowStrategy(template, scriptLoader));
            gcra.add(new GcraStrategy(template, scriptLoader));
        }

        try {
            // ---- Global warm-up: exercise BOTH algorithms so neither is cold
            // when first measured. ----
            flushAll();
            warmUp(sw, "sliding-window");
            warmUp(gcra, "gcra");

            // ---- Latency + throughput scenarios ----
            report.append("## Throughput & latency (higher throughput better; lower latency better)\n\n");
            report.append(String.format("%-42s | %-14s | %12s | %8s | %8s | %8s | %9s | %s%n",
                    "scenario", "algorithm", "thrpt/s(med)", "p50 ms", "p95 ms", "p99 ms", "p999 ms", "allow/deny"));
            report.append("-".repeat(140)).append('\n');

            for (Scenario s : SCENARIOS) {
                TrialAggregate swAgg = runScenario(s, sw, "SLIDING_WINDOW");
                TrialAggregate gcraAgg = runScenario(s, gcra, "GCRA");
                report.append(swAgg.line(s.name(), "SLIDING_WINDOW"));
                report.append(gcraAgg.line(s.name(), "GCRA"));
                report.append(String.format("  -> speedup(GCRA thrpt / SW thrpt) = %.2fx ; p99 ratio(SW/GCRA) = %.2fx%n",
                        gcraAgg.medThroughput / swAgg.medThroughput,
                        swAgg.medP99 / Math.max(gcraAgg.medP99, 1e-9)));
            }

            // ---- Footprint ----
            report.append("\n## Redis memory footprint after filling ")
                  .append(FOOTPRINT_KEYS).append(" keys to limit (search profile: 200/60s)\n\n");
            Footprint swFp = measureFootprint(sw, 200, 60);
            Footprint gcraFp = measureFootprint(gcra, 200, 60);
            report.append(String.format("%-14s | %8s keys | %14s data_bytes | %10s bytes/key%n",
                    "algorithm", "dbsize", "bytes", "approx"));
            report.append("-".repeat(70)).append('\n');
            report.append(swFp.line("SLIDING_WINDOW"));
            report.append(gcraFp.line("GCRA"));
            report.append(String.format("  -> Sliding Window uses %.1fx the data memory of GCRA for this key set%n",
                    (double) swFp.dataBytes() / Math.max(gcraFp.dataBytes(), 1)));

            report.append("\n===BENCH-REPORT-END===\n");
        } finally {
            for (LettuceConnectionFactory f : factories) {
                try { f.destroy(); } catch (Exception ignored) { }
            }
        }

        System.out.print(report);
    }

    // ------------------------------------------------------------------
    // Scenario execution
    // ------------------------------------------------------------------

    private TrialAggregate runScenario(Scenario s, List<RateLimitStrategy> strategies, String label) {
        double[] throughputs = new double[TRIALS];
        double[] p50s = new double[TRIALS];
        double[] p95s = new double[TRIALS];
        double[] p99s = new double[TRIALS];
        double[] p999s = new double[TRIALS];
        long totalAllowed = 0;
        long totalDenied = 0;

        for (int t = 0; t < TRIALS; t++) {
            flushAll();
            hintGc();
            TrialResult r = loadTrial(s, strategies, DURATION_MS);
            throughputs[t] = r.throughputPerSec;
            p50s[t] = r.p50Ms;
            p95s[t] = r.p95Ms;
            p99s[t] = r.p99Ms;
            p999s[t] = r.p999Ms;
            totalAllowed += r.allowed;
            totalDenied += r.denied;
        }

        return new TrialAggregate(
                median(throughputs), min(throughputs), max(throughputs),
                median(p50s), median(p95s), median(p99s), median(p999s),
                totalAllowed, totalDenied);
    }

    /** One measured trial: {@link #CONCURRENCY} workers issue calls as fast as
     *  they can for {@code durationMs}, each recording per-call latency. */
    private TrialResult loadTrial(Scenario s, List<RateLimitStrategy> strategies, long durationMs) {
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch ready = new CountDownLatch(CONCURRENCY);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(CONCURRENCY);
        LongAdder allowed = new LongAdder();
        LongAdder denied = new LongAdder();
        List<LongVec> perThread = new ArrayList<>();
        for (int i = 0; i < CONCURRENCY; i++) perThread.add(new LongVec());

        // Deadline is computed once, on the main thread, and shared -- so every
        // worker stops at the same instant regardless of scheduling skew.
        final long[] deadlineHolder = new long[1];

        for (int i = 0; i < CONCURRENCY; i++) {
            final int workerId = i;
            final RateLimitStrategy strategy = strategies.get(i);
            final LongVec lat = perThread.get(i);
            pool.submit(() -> {
                long localAllowed = 0;
                long localDenied = 0;
                long seq = 0;
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    done.countDown();
                    return;
                }
                long deadline = deadlineHolder[0];
                while (System.nanoTime() < deadline) {
                    String key = keyFor(s, workerId, seq++);
                    long t0 = System.nanoTime();
                    RateLimitDecision d = strategy.isAllowed(key, s.limit(), s.windowSeconds());
                    lat.add(System.nanoTime() - t0);
                    if (d.allowed()) localAllowed++; else localDenied++;
                }
                allowed.add(localAllowed);
                denied.add(localDenied);
                done.countDown();
            });
        }

        try {
            ready.await();
            deadlineHolder[0] = System.nanoTime() + durationMs * 1_000_000L;
            long wallStart = System.nanoTime();
            start.countDown();
            done.await(durationMs + 30_000, TimeUnit.MILLISECONDS);
            long wallElapsedNanos = System.nanoTime() - wallStart;
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);

            long[] all = merge(perThread);
            Arrays.sort(all);
            double thrpt = all.length / (wallElapsedNanos / 1_000_000_000.0);
            return new TrialResult(
                    thrpt,
                    toMillis(percentile(all, 50)),
                    toMillis(percentile(all, 95)),
                    toMillis(percentile(all, 99)),
                    toMillis(percentile(all, 99.9)),
                    allowed.sum(),
                    denied.sum());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
            return new TrialResult(0, 0, 0, 0, 0, 0, 0);
        }
    }

    private void warmUp(List<RateLimitStrategy> strategies, String label) {
        // A hot key AND many keys, both algorithms, long enough to trigger JIT
        // compilation of the hot path and establish all connections.
        Scenario hot = new Scenario("warmup-hot", 200, 60, KeyMode.HOT_KEY);
        Scenario many = new Scenario("warmup-many", 200, 60, KeyMode.MANY_KEYS);
        flushAll();
        loadTrial(hot, strategies, WARMUP_MS);
        flushAll();
        loadTrial(many, strategies, WARMUP_MS);
        System.out.printf("[warmup done: %s]%n", label);
    }

    private String keyFor(Scenario s, int workerId, long seq) {
        return switch (s.keyMode()) {
            case HOT_KEY -> "bench:hot:" + s.limit();
            // Distinct keys, mostly first-touch, so this exercises the admit /
            // key-create path rather than saturating a single key.
            case MANY_KEYS -> "bench:many:" + s.limit() + ":" + workerId + ":" + seq;
        };
    }

    // ------------------------------------------------------------------
    // Footprint
    // ------------------------------------------------------------------

    private Footprint measureFootprint(List<RateLimitStrategy> strategies, int limit, int windowSeconds) {
        flushAll();
        hintGc();
        long baseline = usedMemory();

        // Fill each key up to its limit -- so Sliding Window stores `limit`
        // sorted-set members per key (its worst case) while GCRA stores one
        // scalar per key -- IN PARALLEL across the worker pool. A sequential
        // fill of FOOTPRINT_KEYS * limit ops at this environment's per-op
        // latency can exceed the window, letting early keys' TTLs expire
        // mid-measurement and corrupting the result. Parallelising keeps the
        // whole fill well inside the window.
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch done = new CountDownLatch(CONCURRENCY);
        for (int w = 0; w < CONCURRENCY; w++) {
            final int workerId = w;
            final RateLimitStrategy strategy = strategies.get(w);
            pool.submit(() -> {
                try {
                    for (int k = workerId; k < FOOTPRINT_KEYS; k += CONCURRENCY) {
                        String key = "bench:fp:" + k;
                        for (int i = 0; i < limit; i++) {
                            strategy.isAllowed(key, limit, windowSeconds);
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        try {
            done.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pool.shutdown();

        long dbsize = dbSize();
        long after = usedMemory();
        return new Footprint(dbsize, after, baseline);
    }

    private long usedMemory() {
        return withConnection(conn -> {
            Properties p = conn.serverCommands().info("memory");
            if (p == null) return 0L;
            String v = p.getProperty("used_memory");
            return v == null ? 0L : Long.parseLong(v.trim());
        });
    }

    private long dbSize() {
        return withConnection(conn -> {
            Long n = conn.serverCommands().dbSize();
            return n == null ? 0L : n;
        });
    }

    // ------------------------------------------------------------------
    // Redis admin helpers
    // ------------------------------------------------------------------

    private void flushAll() {
        withConnection(conn -> {
            conn.serverCommands().flushAll();
            return null;
        });
    }

    private <T> T withConnection(java.util.function.Function<RedisConnection, T> fn) {
        RedisConnection conn = redisTemplate.getConnectionFactory().getConnection();
        try {
            return fn.apply(conn);
        } finally {
            conn.close();
        }
    }

    private LettuceConnectionFactory newFactory() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new org.springframework.data.redis.connection.RedisStandaloneConfiguration(
                        REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        return factory;
    }

    private RedisTemplate<String, String> templateFor(LettuceConnectionFactory factory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        template.setKeySerializer(new org.springframework.data.redis.serializer.StringRedisSerializer());
        template.setValueSerializer(new org.springframework.data.redis.serializer.StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    private void hintGc() {
        System.gc();
        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    /** Growable primitive long buffer -- avoids boxing millions of samples. */
    private static final class LongVec {
        private long[] a = new long[1 << 16];
        private int n = 0;
        void add(long v) {
            if (n == a.length) a = Arrays.copyOf(a, a.length << 1);
            a[n++] = v;
        }
    }

    private static long[] merge(List<LongVec> vecs) {
        int total = 0;
        for (LongVec v : vecs) total += v.n;
        long[] all = new long[total];
        int pos = 0;
        for (LongVec v : vecs) {
            System.arraycopy(v.a, 0, all, pos, v.n);
            pos += v.n;
        }
        return all;
    }

    private static double percentile(long[] sorted, double p) {
        if (sorted.length == 0) return 0;
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        index = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[index];
    }

    private static double toMillis(double nanos) { return nanos / 1_000_000.0; }

    private static double median(double[] xs) {
        double[] c = xs.clone();
        Arrays.sort(c);
        int n = c.length;
        if (n == 0) return 0;
        return (n % 2 == 1) ? c[n / 2] : (c[n / 2 - 1] + c[n / 2]) / 2.0;
    }

    private static double min(double[] xs) { double m = Double.MAX_VALUE; for (double x : xs) m = Math.min(m, x); return xs.length == 0 ? 0 : m; }
    private static double max(double[] xs) { double m = -Double.MAX_VALUE; for (double x : xs) m = Math.max(m, x); return xs.length == 0 ? 0 : m; }

    private static int intProp(String name, int def) {
        String v = System.getProperty(name);
        return v == null ? def : Integer.parseInt(v.trim());
    }

    private static long longProp(String name, long def) {
        String v = System.getProperty(name);
        return v == null ? def : Long.parseLong(v.trim());
    }

    private record TrialResult(double throughputPerSec, double p50Ms, double p95Ms,
                                double p99Ms, double p999Ms, long allowed, long denied) {}

    private record TrialAggregate(double medThroughput, double minThroughput, double maxThroughput,
                                   double medP50, double medP95, double medP99, double medP999,
                                   long allowed, long denied) {
        String line(String scenario, String algo) {
            return String.format("%-42s | %-14s | %12.0f | %8.3f | %8.3f | %8.3f | %9.3f | %d/%d%n",
                    truncate(scenario), algo, medThroughput, medP50, medP95, medP99, medP999, allowed, denied);
        }
    }

    private record Footprint(long dbsize, long usedMemoryBytes, long baselineBytes) {
        long dataBytes() { return Math.max(0, usedMemoryBytes - baselineBytes); }
        String line(String algo) {
            long perKey = dbsize == 0 ? 0 : dataBytes() / dbsize;
            return String.format("%-14s | %8d keys | %14d data_bytes | %10d bytes/key%n",
                    algo, dbsize, dataBytes(), perKey);
        }
    }

    private static String truncate(String s) {
        return s.length() <= 42 ? s : s.substring(0, 41) + "…";
    }
}
