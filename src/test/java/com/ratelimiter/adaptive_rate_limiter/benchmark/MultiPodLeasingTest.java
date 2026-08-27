package com.ratelimiter.adaptive_rate_limiter.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.ratelimiter.adaptive_rate_limiter.config.RateLimiterProperties;
import com.ratelimiter.adaptive_rate_limiter.metrics.RateLimiterMetrics;
import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;
import com.ratelimiter.adaptive_rate_limiter.service.quota.AdaptiveLeaseController;
import com.ratelimiter.adaptive_rate_limiter.service.quota.GcraQuotaAllocator;
import com.ratelimiter.adaptive_rate_limiter.service.quota.LeaseManager;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.GcraStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.support.RedisStrategyTestBase;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Phase 3E -- multi-pod correctness invariants for quota leasing, plus a
 * decision-grade Phase 2 (direct GCRA) vs Phase 3 (leased GCRA) benchmark whose
 * headline is <b>Redis calls saved</b>.
 *
 * <p>Each simulated pod is a fully independent {@link LeaseManager} with its own
 * {@link LocalQuotaBucket} state and its own {@link LettuceConnectionFactory}
 * (its own client-side connection), all pointed at the <b>one shared Redis</b> of
 * {@link RedisStrategyTestBase}. Because every pod's allocator derives the same
 * Redis key ({@code ratelimit:<key>:gcra}) for a given logical key, the pods
 * coordinate through a <b>single shared GCRA TAT</b> -- which is the whole point:
 * the batch-GCRA reservation, not a naive counter, is what keeps P pods' combined
 * output paced to the global limit.
 *
 * <h2>Guarantee semantics (deliberately NOT rolling-window exactness)</h2>
 * Leasing is applied only to RATE/PACING (GCRA) endpoints, which guarantee a
 * <em>long-run rate</em> plus a <em>bounded burst</em>, never an exact
 * rolling-window ceiling. So the invariants below assert:
 * <ul>
 *   <li>{@link #multiPodSustainedAggregateRateConvergesToLimit()} -- the
 *       <b>sustained aggregate rate</b> across P pods converges to
 *       {@code limit/window} (and is nowhere near {@code P x} that), NOT that any
 *       rolling window holds {@code <= limit}; and</li>
 *   <li>{@link #instantaneousOvershootStaysWithinPTimesMaxLeaseFractionLimit()}
 *       -- the <b>instantaneous</b> admit burst stays within
 *       {@code P x maxLeaseFraction x limit} (each of P pods can hold at most one
 *       lease of {@code maxLeaseFraction x limit} unspent units). This bound may
 *       exceed {@code limit}; the test explicitly does <b>not</b> assert
 *       {@code <= limit}.</li>
 * </ul>
 *
 * <p>The three {@code @Test} invariants are bounded (a few seconds) and run in a
 * normal {@code mvn test}. The heavy benchmark is opt-in: it is
 * {@link Assumptions#assumeTrue assumed-skipped} unless {@code -Dbench.multipod}
 * is set, e.g.:
 * <pre>
 *   sh mvnw -Dtest=MultiPodLeasingTest -DfailIfNoTests=false -Dsurefire.useFile=false \
 *           -Dbench.multipod=true test
 * </pre>
 * Benchmark tunables: {@code -Dbench.pods}, {@code -Dbench.requests},
 * {@code -Dbench.limit}, {@code -Dbench.window}, {@code -Dbench.maxLeaseFraction}.
 */
class MultiPodLeasingTest extends RedisStrategyTestBase {

    // ------------------------------------------------------------------
    // Correctness invariants (always run; deterministic, self-contained config)
    // ------------------------------------------------------------------

    /**
     * Test 1 (single pod) -- leasing collapses {@code N} admits into {@code N/K}
     * Redis calls. With {@code limit = 1000} the idle-key burst budget is exactly
     * 1000, so all 1000 requests admit; with {@code K = 10} that costs exactly
     * {@code 1000/10 = 100} synchronous leases instead of 1000 direct calls -- 900
     * Redis round trips saved. Prefetch is off, so batches drain fully and the
     * count is exact.
     */
    @Test
    void singlePodCollapsesRedisCallsToAdmitsOverK() {
        int limit = 1000;
        int window = 60;
        double frac = 0.01;               // K = round(0.01 * 1000) = 10
        int k = kFor(limit, frac);
        int requests = 1000;              // == idle-key burst budget -> all admit

        flushAll();
        Phase3Pod pod = newPhase3Pod(limit, window, frac);
        try {
            String key = uniqueKey("mp:single");
            int admitted = 0;
            for (int i = 0; i < requests; i++) {
                RateLimitDecision d = pod.manager.tryAdmit(key, limit, window);
                assertNotNull(d, "healthy Redis must never signal degrade");
                if (d.allowed()) {
                    admitted++;
                }
            }

            long redisCalls = pod.manager.syncLeaseCount() + pod.manager.prefetchLeaseCount();
            long localAdmits = pod.manager.localAdmitCount();

            assertEquals(requests, admitted,
                    "idle-key burst budget == limit, so all " + requests + " requests admit");
            assertEquals(requests / k, redisCalls,
                    "one Redis call per K=" + k + " admits: " + requests + " admits -> "
                            + (requests / k) + " leases");
            assertEquals(admitted, redisCalls + localAdmits,
                    "every admit is either the one that drew a lease or a local (no-Redis) admit");

            long saved = requests - redisCalls; // vs Phase 2's one call per request
            assertTrue(saved >= requests * 9L / 10,
                    "leasing must save >=90% of Redis calls here; saved=" + saved + "/" + requests);
            System.out.printf("[Test1] requests=%d admitted=%d redisCalls=%d saved=%d (%.1f%%)%n",
                    requests, admitted, redisCalls, saved, 100.0 * saved / requests);
        } finally {
            pod.close();
        }
    }

    /**
     * Test 2 (multi-pod aggregate) -- P pods hammering one shared key sustain a
     * combined output of about {@code limit/window}, because every lease advances
     * the same shared TAT. Phrased per the semantics table: the assertion is on the
     * <b>sustained aggregate rate</b> (long-run), and critically that it is far
     * below the {@code P x} rate that P uncoordinated limiters would emit -- NOT
     * that any rolling window holds {@code <= limit}.
     */
    @Test
    void multiPodSustainedAggregateRateConvergesToLimit() {
        int pods = 5;
        int limit = 100;
        int window = 1;                   // rate target = 100 admits/sec (shared)
        double frac = 0.1;                // K = 10
        long durationMs = 3000;

        flushAll();
        List<Phase3Pod> fleet = newFleet(pods, limit, window, frac);
        try {
            String key = uniqueKey("mp:aggregate");
            ConcurrentResult r = runConcurrentLeased(fleet, key, limit, window, durationMs);

            double seconds = durationMs / 1000.0;
            double aggregateRate = r.admitted / seconds;
            double limitRate = (double) limit / window;                 // 100/s
            double expectedWithBurst = limit * (1 + seconds);           // burst + steady over the run
            double steadyFloor = limit * seconds * 0.6;                 // generous long-run lower bound
            double uncoordinated = pods * expectedWithBurst;            // what P independent limiters emit

            assertTrue(r.admitted <= expectedWithBurst * 1.3,
                    "aggregate admits " + r.admitted + " must stay near burst+steady (~"
                            + Math.round(expectedWithBurst) + ")");
            assertTrue(r.admitted >= steadyFloor,
                    "aggregate must sustain ~limit rate; admits=" + r.admitted
                            + " floor=" + Math.round(steadyFloor));
            assertTrue(r.admitted < uncoordinated * 0.5,
                    "coordination proof: aggregate " + r.admitted
                            + " must be far below P x limit rate (" + Math.round(uncoordinated) + ")");
            System.out.printf(
                    "[Test2] pods=%d limit=%d/%ds admits=%d over %.1fs -> aggregateRate=%.0f/s (target %.0f/s)%n",
                    pods, limit, window, r.admitted, seconds, aggregateRate, limitRate);
        } finally {
            closeFleet(fleet);
        }
    }

    /**
     * Test 3 (overshoot) -- with each of P pods able to hold at most one lease of
     * {@code K = maxLeaseFraction x limit} unspent units, the largest admit burst
     * that can occur in an instant is {@code P x K}. We measure the peak admits in
     * any short sliding window across all pods and assert it stays within that
     * bound (plus a small slack for the <=1 fresh GCRA emission per pod that can
     * land inside the measurement window). Prefetch is off so no bucket ever holds
     * more than K.
     *
     * <p>Here {@code P x K = 5 x 25 = 125 > limit = 100}, so the observed peak may
     * exceed {@code limit} -- and per the semantics constraint this test
     * deliberately does <b>not</b> assert {@code <= limit}.
     */
    @Test
    void instantaneousOvershootStaysWithinPTimesMaxLeaseFractionLimit() {
        int pods = 5;
        int limit = 100;
        int window = 1;
        double frac = 0.25;               // K = 25 -> P*K = 125 > limit = 100
        int k = kFor(limit, frac);
        long durationMs = 2000;
        long instantMs = 10;              // ~ one emission interval (period/limit = 10ms)

        flushAll();
        List<Phase3Pod> fleet = newFleet(pods, limit, window, frac);
        try {
            String key = uniqueKey("mp:overshoot");
            ConcurrentResult r = runConcurrentLeased(fleet, key, limit, window, durationMs);

            int peakInstant = maxInWindow(r.admitTimesNanos, instantMs * 1_000_000L);
            int bound = pods * k;         // P x maxLeaseFraction x limit
            int slack = pods;             // <=1 fresh emission per pod within the measurement window

            assertTrue(peakInstant <= bound + slack,
                    "instantaneous admits " + peakInstant
                            + " must stay within P x maxLeaseFraction x limit (" + bound
                            + ") + slack " + slack);
            // NOTE: intentionally NOT asserting peakInstant <= limit; overshoot up to P*K is by design.
            System.out.printf(
                    "[Test3] pods=%d K=%d bound(P*K)=%d limit=%d observedInstantMax=%d (exceeds limit? %b)%n",
                    pods, k, bound, limit, peakInstant, peakInstant > limit);
        } finally {
            closeFleet(fleet);
        }
    }

    // ------------------------------------------------------------------
    // Phase 2 vs Phase 3 benchmark (opt-in via -Dbench.multipod)
    // ------------------------------------------------------------------

    private static final int BENCH_PODS = intProp("bench.pods", 10);
    private static final int BENCH_REQUESTS = intProp("bench.requests", 100_000);
    private static final int BENCH_LIMIT = intProp("bench.limit", 1000);
    private static final int BENCH_WINDOW = intProp("bench.window", 60);
    private static final double BENCH_FRAC = doubleProp("bench.maxLeaseFraction", 0.1); // K = 100

    @Test
    void phase2VsPhase3Benchmark() {
        Assumptions.assumeTrue(System.getProperty("bench.multipod") != null,
                "multi-pod benchmark is opt-in: run with -Dbench.multipod=true");

        int k = kFor(BENCH_LIMIT, BENCH_FRAC);
        StringBuilder report = new StringBuilder();
        report.append("\n===BENCH-REPORT-START===\n");
        report.append("# Phase 2 (direct GCRA) vs Phase 3 (leased GCRA) -- multi-pod\n\n");
        report.append(String.format("config: pods=%d requests=%d limit=%d/%ds K=%d (maxLeaseFraction=%.3f)%n",
                BENCH_PODS, BENCH_REQUESTS, BENCH_LIMIT, BENCH_WINDOW, k, BENCH_FRAC));
        report.append("redis: shared Testcontainers redis:7-alpine; each pod has its own connection\n");
        report.append("headline: Redis calls saved = phase2 calls (1/req) - phase3 calls (leases + empty-budget probes)\n\n");

        // HEALTHY: spread requests over enough keys that each key stays within its
        // burst budget -> ~all admit -> leasing batches admits into few calls.
        int healthyKeys = Math.max(1, BENCH_REQUESTS / BENCH_LIMIT);
        // ABUSE: one hot key hammered far past its budget -> most requests denied;
        // denials still cost a Redis probe in both phases, so savings are modest.
        int abuseKeys = 1;

        report.append(renderScenario("HEALTHY (sub-limit, " + healthyKeys + " keys)", healthyKeys));
        report.append(renderScenario("ABUSE (over-limit, 1 hot key)", abuseKeys));
        report.append("\n===BENCH-REPORT-END===\n");
        System.out.print(report);
    }

    private String renderScenario(String name, int numKeys) {
        List<String> keys = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++) {
            keys.add("bench:mp:" + numKeys + ":" + i);
        }

        flushAll();
        BenchResult direct = benchDirect(keys);
        flushAll();
        BenchResult leased = benchLeased(keys);

        long saved = direct.redisCalls - leased.redisCalls;
        double savedPct = direct.redisCalls == 0 ? 0 : 100.0 * saved / direct.redisCalls;

        StringBuilder sb = new StringBuilder();
        sb.append("## ").append(name).append('\n');
        sb.append(String.format("%-14s | %10s | %10s | %10s | %12s | %8s | %8s | %8s%n",
                "phase", "requests", "admitted", "denied", "redis_calls", "p50 ms", "p95 ms", "p99 ms"));
        sb.append("-".repeat(96)).append('\n');
        sb.append(direct.line("PHASE2 direct"));
        sb.append(leased.line("PHASE3 leased"));
        sb.append(String.format(">> Redis calls saved: %d of %d (%.1f%%) ; global admits: phase2=%d phase3=%d%n",
                saved, direct.redisCalls, savedPct, direct.admitted, leased.admitted));
        // The instantaneous overshoot bound P x maxLeaseFraction x limit is PER KEY (each of P
        // pods can hold one lease of K unspent units for a key). Only compare against it directly
        // for the single-key scenario; for a multi-key run the peak is an aggregate across keys,
        // legitimately bounded by numKeys x P x K, not P x K.
        int pk = BENCH_PODS * kFor(BENCH_LIMIT, BENCH_FRAC);
        if (numKeys == 1) {
            sb.append(String.format(
                    ">> Phase 3 instantaneous overshoot (max admits/10ms, 1 key) = %d ; per-key bound P*K = %d ; limit = %d%n%n",
                    leased.peakInstant, pk, BENCH_LIMIT));
        } else {
            sb.append(String.format(
                    ">> Phase 3 aggregate peak admits/10ms across %d keys = %d ; per-key bound P*K = %d ; aggregate bound = %d%n%n",
                    numKeys, leased.peakInstant, pk, (long) numKeys * pk));
        }
        return sb.toString();
    }

    /** Phase 2: every request is one direct GCRA {@code EVAL} -> redis calls == requests. */
    private BenchResult benchDirect(List<String> keys) {
        List<Phase2Pod> fleet = new ArrayList<>();
        for (int i = 0; i < BENCH_PODS; i++) {
            fleet.add(newPhase2Pod());
        }
        try {
            LongAdder admitted = new LongAdder();
            LongAdder denied = new LongAdder();
            List<LongVec> lat = fixedCountRun(fleet.size(), (podId, latVec) -> {
                GcraStrategy s = fleet.get(podId).strategy;
                long a = 0;
                long d = 0;
                for (int seq = podId; seq < BENCH_REQUESTS; seq += BENCH_PODS) {
                    String key = keys.get(seq % keys.size());
                    long t0 = System.nanoTime();
                    RateLimitDecision dec = s.isAllowed(key, BENCH_LIMIT, BENCH_WINDOW);
                    latVec.add(System.nanoTime() - t0);
                    if (dec.allowed()) {
                        a++;
                    } else {
                        d++;
                    }
                }
                admitted.add(a);
                denied.add(d);
            });
            long[] all = sortedLatencies(lat);
            return new BenchResult(all.length, admitted.sum(), denied.sum(),
                    all.length /* one call per request */, all, 0);
        } finally {
            fleet.forEach(Phase2Pod::close);
        }
    }

    /** Phase 3: leased admission; redis calls == sync + prefetch leases across all pods. */
    private BenchResult benchLeased(List<String> keys) {
        List<Phase3Pod> fleet = newFleet(BENCH_PODS, BENCH_LIMIT, BENCH_WINDOW, BENCH_FRAC);
        try {
            LongAdder admitted = new LongAdder();
            LongAdder denied = new LongAdder();
            List<LongVec> admitTimes = new ArrayList<>();
            for (int i = 0; i < BENCH_PODS; i++) {
                admitTimes.add(new LongVec());
            }
            List<LongVec> lat = fixedCountRun(fleet.size(), (podId, latVec) -> {
                LeaseManager m = fleet.get(podId).manager;
                LongVec times = admitTimes.get(podId);
                long a = 0;
                long d = 0;
                for (int seq = podId; seq < BENCH_REQUESTS; seq += BENCH_PODS) {
                    String key = keys.get(seq % keys.size());
                    long t0 = System.nanoTime();
                    RateLimitDecision dec = m.tryAdmit(key, BENCH_LIMIT, BENCH_WINDOW);
                    long t1 = System.nanoTime();
                    latVec.add(t1 - t0);
                    if (dec != null && dec.allowed()) {
                        a++;
                        times.add(t1);
                    } else {
                        d++;
                    }
                }
                admitted.add(a);
                denied.add(d);
            });

            long redisCalls = 0;
            for (Phase3Pod p : fleet) {
                redisCalls += p.manager.syncLeaseCount() + p.manager.prefetchLeaseCount();
            }
            long[] all = sortedLatencies(lat);
            long[] admits = merge(admitTimes);
            Arrays.sort(admits);
            int peak = maxInWindow(admits, 10 * 1_000_000L);
            return new BenchResult(all.length, admitted.sum(), denied.sum(), redisCalls, all, peak);
        } finally {
            closeFleet(fleet);
        }
    }

    // ------------------------------------------------------------------
    // Concurrency harness
    // ------------------------------------------------------------------

    /** A worker body: given its pod id and a latency buffer, issue its share of requests. */
    private interface Worker {
        void run(int podId, LongVec latencies);
    }

    /**
     * Runs {@code podCount} workers concurrently behind a start barrier (so all
     * begin at once), each recording latencies into its own buffer, and returns
     * the per-worker buffers once every worker has finished its fixed request quota.
     */
    private List<LongVec> fixedCountRun(int podCount, Worker worker) {
        ExecutorService pool = Executors.newFixedThreadPool(podCount);
        CountDownLatch ready = new CountDownLatch(podCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(podCount);
        List<LongVec> perPod = new ArrayList<>();
        for (int i = 0; i < podCount; i++) {
            perPod.add(new LongVec());
        }
        for (int i = 0; i < podCount; i++) {
            final int podId = i;
            final LongVec lat = perPod.get(i);
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    worker.run(podId, lat);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        try {
            ready.await();
            start.countDown();
            done.await(5, TimeUnit.MINUTES);
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
        return perPod;
    }

    /**
     * Closed-loop: {@code fleet.size()} pods hammer {@code key} as fast as they can
     * for {@code durationMs} (behind a start barrier), recording every admit's
     * timestamp. Returns aggregate admits/denies and the sorted admit timestamps
     * (for the instantaneous-overshoot measurement).
     */
    private ConcurrentResult runConcurrentLeased(List<Phase3Pod> fleet, String key,
                                                 int limit, int window, long durationMs) {
        int p = fleet.size();
        ExecutorService pool = Executors.newFixedThreadPool(p);
        CountDownLatch ready = new CountDownLatch(p);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(p);
        LongAdder admitted = new LongAdder();
        LongAdder denied = new LongAdder();
        List<LongVec> admitTimes = new ArrayList<>();
        for (int i = 0; i < p; i++) {
            admitTimes.add(new LongVec());
        }
        final long[] deadline = new long[1];

        for (int i = 0; i < p; i++) {
            final LeaseManager mgr = fleet.get(i).manager;
            final LongVec times = admitTimes.get(i);
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    done.countDown();
                    return;
                }
                long a = 0;
                long d = 0;
                while (System.nanoTime() < deadline[0]) {
                    RateLimitDecision dec = mgr.tryAdmit(key, limit, window);
                    long t = System.nanoTime();
                    if (dec != null && dec.allowed()) {
                        a++;
                        times.add(t);
                    } else {
                        d++;
                    }
                }
                admitted.add(a);
                denied.add(d);
                done.countDown();
            });
        }

        try {
            ready.await();
            deadline[0] = System.nanoTime() + durationMs * 1_000_000L;
            start.countDown();
            done.await(durationMs + 30_000, TimeUnit.MILLISECONDS);
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }

        long[] all = merge(admitTimes);
        Arrays.sort(all);
        return new ConcurrentResult(admitted.sum(), denied.sum(), all);
    }

    // ------------------------------------------------------------------
    // Pod construction
    // ------------------------------------------------------------------

    /** One simulated Phase 3 pod: an independent LeaseManager + its own connection. */
    private static final class Phase3Pod {
        final LeaseManager manager;
        final LettuceConnectionFactory factory;

        Phase3Pod(LeaseManager manager, LettuceConnectionFactory factory) {
            this.manager = manager;
            this.factory = factory;
        }

        void close() {
            try {
                factory.destroy();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    /** One simulated Phase 2 pod: a direct GcraStrategy + its own connection. */
    private static final class Phase2Pod {
        final GcraStrategy strategy;
        final LettuceConnectionFactory factory;

        Phase2Pod(GcraStrategy strategy, LettuceConnectionFactory factory) {
            this.strategy = strategy;
            this.factory = factory;
        }

        void close() {
            try {
                factory.destroy();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    private Phase3Pod newPhase3Pod(int limit, int window, double frac) {
        LettuceConnectionFactory factory = newFactory();
        RedisTemplate<String, String> template = templateFor(factory);
        RateLimiterProperties props = leasedProps(window, frac);
        GcraQuotaAllocator allocator = new GcraQuotaAllocator(template, new LuaScriptLoader(), props);
        LeaseManager manager = new LeaseManager(allocator, props, CircuitBreakerRegistry.ofDefaults(),
                new AdaptiveLeaseController(props), new RateLimiterMetrics(new SimpleMeterRegistry()));
        return new Phase3Pod(manager, factory);
    }

    private Phase2Pod newPhase2Pod() {
        LettuceConnectionFactory factory = newFactory();
        RedisTemplate<String, String> template = templateFor(factory);
        return new Phase2Pod(new GcraStrategy(template, new LuaScriptLoader()), factory);
    }

    private List<Phase3Pod> newFleet(int pods, int limit, int window, double frac) {
        List<Phase3Pod> fleet = new ArrayList<>(pods);
        for (int i = 0; i < pods; i++) {
            fleet.add(newPhase3Pod(limit, window, frac));
        }
        return fleet;
    }

    private void closeFleet(List<Phase3Pod> fleet) {
        fleet.forEach(Phase3Pod::close);
    }

    /**
     * Leasing properties for a pod: feature on, fixed K via {@code maxLeaseFraction},
     * prefetch OFF (so batches drain fully and per-bucket holdings never exceed K --
     * both make the invariant counts deterministic), long lease TTL.
     */
    private RateLimiterProperties leasedProps(int window, double frac) {
        RateLimiterProperties props = new RateLimiterProperties();
        props.setWindowSizeSeconds(window);
        RateLimiterProperties.Leasing leasing = props.getLeasing();
        leasing.setEnabled(true);
        leasing.setMinLease(1);
        leasing.setMaxLeaseFraction(frac);
        leasing.setPrefetchWatermark(0.0);
        leasing.setLeaseTtlMs(60_000);
        return props;
    }

    /** Fixed lease size K, computed exactly as {@code LeaseManager.leaseSizeFor}. */
    private static int kFor(int limit, double frac) {
        int k = (int) Math.round(frac * limit);
        k = Math.max(1, k);
        return Math.min(k, Math.max(1, limit));
    }

    // ------------------------------------------------------------------
    // Redis connection helpers (mirror GcraVsSlidingWindowDecisionBenchmark)
    // ------------------------------------------------------------------

    private LettuceConnectionFactory newFactory() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        return factory;
    }

    private RedisTemplate<String, String> templateFor(LettuceConnectionFactory factory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    private void flushAll() {
        RedisConnection conn = redisTemplate.getConnectionFactory().getConnection();
        try {
            conn.serverCommands().flushAll();
        } finally {
            conn.close();
        }
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    /** Growable primitive long buffer -- avoids boxing millions of samples. */
    private static final class LongVec {
        private long[] a = new long[1 << 16];
        private int n = 0;

        void add(long v) {
            if (n == a.length) {
                a = Arrays.copyOf(a, a.length << 1);
            }
            a[n++] = v;
        }
    }

    private static long[] merge(List<LongVec> vecs) {
        int total = 0;
        for (LongVec v : vecs) {
            total += v.n;
        }
        long[] all = new long[total];
        int pos = 0;
        for (LongVec v : vecs) {
            System.arraycopy(v.a, 0, all, pos, v.n);
            pos += v.n;
        }
        return all;
    }

    private static long[] sortedLatencies(List<LongVec> vecs) {
        long[] all = merge(vecs);
        Arrays.sort(all);
        return all;
    }

    /** Max number of samples inside any half-open window of {@code widthNanos}. */
    private static int maxInWindow(long[] sortedNanos, long widthNanos) {
        int max = 0;
        int lo = 0;
        for (int hi = 0; hi < sortedNanos.length; hi++) {
            while (sortedNanos[hi] - sortedNanos[lo] >= widthNanos) {
                lo++;
            }
            max = Math.max(max, hi - lo + 1);
        }
        return max;
    }

    private static double percentileMs(long[] sortedNanos, double p) {
        if (sortedNanos.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(p / 100.0 * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.length - 1));
        return sortedNanos[index] / 1_000_000.0;
    }

    private static int intProp(String name, int def) {
        String v = System.getProperty(name);
        return v == null ? def : Integer.parseInt(v.trim());
    }

    private static double doubleProp(String name, double def) {
        String v = System.getProperty(name);
        return v == null ? def : Double.parseDouble(v.trim());
    }

    private record ConcurrentResult(long admitted, long denied, long[] admitTimesNanos) {}

    private record BenchResult(long requests, long admitted, long denied, long redisCalls,
                               long[] sortedLatNanos, int peakInstant) {
        String line(String phase) {
            return String.format("%-14s | %10d | %10d | %10d | %12d | %8.3f | %8.3f | %8.3f%n",
                    phase, requests, admitted, denied, redisCalls,
                    percentileMs(sortedLatNanos, 50), percentileMs(sortedLatNanos, 95),
                    percentileMs(sortedLatNanos, 99));
        }
    }
}
