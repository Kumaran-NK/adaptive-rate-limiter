# GCRA vs Sliding Window — Decision-Grade Benchmark

**Adaptive Distributed Rate Limiter · algorithm selection study**
_Real Redis (Testcontainers `redis:7-alpine`) · 5 trials/scenario · p50/p95/p99/p999 · 2026-08-24_

---

## TL;DR — the money slide

> **Recommendation: classify each endpoint. Route EXACT_QUOTA endpoints (payment, sms) to Sliding Window and RATE/PACING endpoints (search, ai-inference) to GCRA — rather than choosing one universal default.**
> The deciding factor is **memory, not speed**: GCRA uses **~207× less Redis memory** for the same key set. Throughput/latency is effectively a wash on this rig (GCRA ~6–9% slower per-op server-side, within transport noise). Semantics differ and that difference — not performance — is what should drive per-endpoint choice.

| Axis | Winner | Margin | Confidence |
|---|---|---|---|
| **Memory footprint** | **GCRA** | **~207×** (120 B/key vs 24,933 B/key) | High — structural, provable from the Lua |
| Throughput (sustained load) | Sliding Window | ~6–9% | Low — flips between runs, transport-dominated |
| Tail latency (p95/p99) | Sliding Window | small | Low — rig-dependent |
| Burst smoothing / recovery | **GCRA** | qualitative | High — algorithmic |
| Rolling-window correctness | **Sliding Window** | GCRA can reach ~2× limit in a rolling window (by design) | High — algorithmic |

---

## 1. The question

The project ships **two** distributed rate-limiting strategies backed by Redis + atomic Lua:

- **Sliding Window** — an exact rolling count kept in a Redis **sorted set** (one member per request).
- **GCRA** (Generic Cell Rate Algorithm) — a single **TAT scalar** (Theoretical Arrival Time) per key, smooth pacing.

Sliding Window is the one actually wired into production today. GCRA is fully implemented but not yet on any request path. **Should GCRA become the default distributed strategy?** This study answers that with decision-grade numbers instead of intuition.

---

## 2. What was done

1. **Audited the live code path** to establish ground truth (findings in §6).
2. **Built a hardened, decision-grade benchmark** — [GcraVsSlidingWindowDecisionBenchmark.java](adaptive-rate-limiter/src/test/java/com/ratelimiter/adaptive_rate_limiter/benchmark/GcraVsSlidingWindowDecisionBenchmark.java) — designed to remove the cold-start bias present in the original directional benchmark.
3. **Ran it against real Redis** (Testcontainers), 5 trials/scenario, and captured p50/p95/p99/p999 + Redis memory footprint.
4. **Analysed** performance, memory, and semantics, and turned it into the recommendation above.

---

## 3. How the two algorithms work (and why memory diverges)

Both do exactly **one Redis round trip per decision** (a single atomic `EVAL`). The difference is what they store.

### Sliding Window — [sliding_window.lua](adaptive-rate-limiter/src/main/resources/lua/sliding_window.lua)
```
ZREMRANGEBYSCORE key 0 (now-window)   -- drop expired
ZCARD key                             -- count live requests
if count < limit:  ZADD key now <request-id>   -- store THIS request
                   PEXPIRE key window+1s
```
Stores **one sorted-set member per admitted request** → memory is **O(limit)** per key. Exact: the count is a true rolling window.

### GCRA — [gcra.lua](adaptive-rate-limiter/src/main/resources/lua/gcra.lua)
```
now = redis.call('TIME')              -- server clock, no clock-skew across nodes
emission_interval = period_ms / limit
tat = GET key  (or now if idle/stale)
if tat + emission_interval - period_ms <= now:  SET key (tat+emission_interval)
```
Stores **a single scalar (the TAT)** → memory is **O(1)** per key, regardless of limit. Smooth: capacity drips back continuously at `period/limit`.

> **This is the whole memory story.** A `payment` key at its limit holds 10 sorted-set members under Sliding Window but one number under GCRA; a `search` key holds up to 200 members vs one number. GCRA also self-expires the key the instant its TAT decays to "now".

---

## 4. Benchmark methodology (why these numbers are trustworthy)

The original directional benchmark ran Sliding Window first from cold and reported GCRA as faster — a JIT/connection/Lua-cache cold-start artifact. The hardened harness removes that:

| Technique | Why |
|---|---|
| **Warm-up both algorithms** (hot-key + many-keys, 2 s each) before any measurement | Eliminates JIT / connection-pool / Lua-script-cache cold start |
| **Per-worker dedicated connection + strategy** (16 workers) | Surfaces real server-side cost; no shared-connection serialization masking it |
| **`flushAll()` + GC hint before *every* trial** | Each trial starts clean — **no cross-algorithm key contamination**, no GC carryover |
| **Closed-loop sustained load**, shared deadline across workers | Measures steady-state throughput, not a burst |
| **5 trials, median reported** + min/max | Dampens run-to-run variance |
| **p50 / p95 / p99 / p999** | Tail latency, not just averages |
| **Real Redis** via Testcontainers | Real Lua execution, real memory accounting |

**One EVAL per decision for both** ⇒ network RTT is an identical constant added to each, so the **GCRA-vs-SW delta is RTT-independent** — it reflects Redis server-side work + Java serialization, the part that actually differs.

**Honest environment caveat:** Redis runs locally over a Windows named-pipe Docker socket, so absolute per-op latency (~2–4 ms) is **transport-dominated**. Small throughput/latency deltas here are near the rig's noise floor. Memory numbers are unaffected by transport and are the robust signal.

Config: `trials=5 durationMs=3000 warmupMs=2000 concurrency=16 footprintKeys=1000`.

---

## 5. Results

### 5.1 Throughput & latency (5-trial medians)

| Scenario | Algorithm | Thrpt/s (med) | p50 ms | p95 ms | p99 ms | p999 ms | allowed/denied |
|---|---|--:|--:|--:|--:|--:|--:|
| Many clients (payment 10/60s) | Sliding Window | **5804** | **2.457** | **4.658** | **7.414** | **14.010** | 87060/0 |
| Many clients (payment 10/60s) | GCRA | 4184 | 3.098 | 7.431 | 13.261 | 34.505 | 57742/0 |
| Many clients (search 200/60s) | Sliding Window | **5615** | **2.445** | 5.293 | **9.344** | **21.162** | 85573/0 |
| Many clients (search 200/60s) | GCRA | 5124 | 2.556 | **5.348** | 12.129 | 23.868 | 76741/0 |
| Hot key under abuse (search 200/60s) | Sliding Window | **5941** | **2.365** | **4.668** | **7.464** | **13.991** | 1000/86705 |
| Hot key under abuse (search 200/60s) | GCRA | 5545 | 2.479 | 5.433 | 9.732 | 17.979 | 1045/81219 |
| Hot key, high limit (synthetic 5000/60s) | Sliding Window | **5670** | **2.401** | **4.988** | 8.120 | 25.944 | 25000/62819 |
| Hot key, high limit (synthetic 5000/60s) | GCRA | 5341 | 2.733 | 5.124 | **8.170** | **18.526** | 26245/48715 |

**Read this honestly:**
- Sliding Window edged GCRA on median throughput in **all four** scenarios (GCRA at 0.72×, 0.91×, 0.93×, 0.94×).
- **But the ranking is not stable.** A quick 2-trial run showed GCRA *faster* (payment 1.47×, search 1.07×). And the two *structurally identical* MANY_KEYS scenarios (payment & search are both "one request per unique key, always allowed") disagree — 0.72× vs 0.91× — which means the 0.72× payment figure is **variance, not algorithm**.
- The **reproducible** signal is a **small ~6–9% server-side throughput penalty for GCRA** (the 0.91–0.94× scenarios), consistent with GCRA's one extra in-script `TIME` call + floating-point math per decision. Sliding Window also had slightly tighter p95/p99 tails in most scenarios.
- All p50s cluster in **2.4–3.1 ms** — dominated by the ~2–4 ms transport floor. This is **not** a decision-grade latency separation.

**Takeaway: performance is a wash. It does not, on its own, justify switching — or refusing to switch.**

### 5.2 Redis memory footprint — the decisive axis

Filled 1000 distinct keys to the `search` profile (limit 200), measured data bytes (empty-Redis baseline subtracted):

| Algorithm | Keys | Data bytes | Bytes/key |
|---|--:|--:|--:|
| Sliding Window | 1000 | 24,933,048 | **24,933** |
| GCRA | 1000 | 120,664 | **120** |

> **Sliding Window uses 206.6× the memory of GCRA for the same key set.**
> Sliding Window's per-key cost scales with the limit (~125 B per stored request member); GCRA's is a flat ~120 B no matter the limit. At scale — many clients × high limits — this is the difference between a rate limiter that fits in Redis and one that doesn't.

### 5.3 Semantics — the tie-breaker that actually matters

**This is where GCRA and Sliding Window are genuinely _not_ interchangeable**, and it outweighs the performance wash. With **DVT = period** (the configured choice), GCRA is a **burst allowance + sustained pacing** limiter — *not* an exact rolling-window counter.

| Property (limit 10 / 1 s example) | Sliding Window | GCRA (DVT = period) |
|---|---|---|
| Burst against an idle key | up to 10 | **exactly 10** |
| Sustained long-run rate | 10 / s | **10 / s** (1 every 100 ms) |
| Count in **any** rolling 1 s window | **never exceeds 10** | **can reach ~19 (≈ 2·limit − 1)** |
| Recovery after a partial burst | must wait out the window | **recovers continuously (faster)** |
| Sustained-phase tail latency (scenario D) | p99 15.6 ms | **p99 5.7 ms** |

**Why the rolling count exceeds the limit:** after admitting a full burst of 10 against an idle key at t=0, GCRA's TAT sits at t=1000 ms and it immediately re-admits 1 request every emission interval (100 ms) — it does **not** wait for the window to clear. So the rolling window [0, 1000 ms) holds the 10-request burst **plus** ~9 paced admits ≈ 19. This is **by design** — a direct consequence of setting DVT = period so that "burst capacity == limit" — not a bug.

> **Correction to an earlier draft.** A previous version of this document said "over a full window both admit exactly limit." That is wrong for a *rolling* window and is corrected here. GCRA's *sustained rate* converges to limit/period, but its *rolling-window count* can transiently reach ~2·limit right after an idle burst. **Sliding Window is the only one of the two that guarantees "no more than N in any rolling window."** The earlier "110–111 admitted" figure (limit 100 / 1 s) was measured over a ~150 ms burst, so it captured only the initial 100 + ~11 paced admits; a full second of sustained pressure approaches ~200.

**What this means per endpoint:**
- **EXACT_QUOTA** — the limit is a hard, auditable ceiling ("no more than N per window": payment, sms) → **Sliding Window**. GCRA would let a caller exceed the nominal quota within a rolling window.
- **RATE / PACING** — the goal is to shield a downstream from sustained overload, not to enforce an exact count (search, ai-inference) → **GCRA**. Smooth pacing, no boundary cliff-release, and the ~2× rolling transient is harmless.

---

## 6. Codebase audit findings (ground truth)

Surfaced while verifying what actually runs today. These are independent of the benchmark and worth a slide:

- **GCRA is implemented but not wired.** [RateLimiterService.java](adaptive-rate-limiter/src/main/java/com/ratelimiter/adaptive_rate_limiter/service/RateLimiterService.java) hardcodes `slidingWindowStrategy.isAllowed(...)`; [GcraStrategy.java](adaptive-rate-limiter/src/main/java/com/ratelimiter/adaptive_rate_limiter/service/strategy/GcraStrategy.java) is a `@Component` that no production path calls. Adopting GCRA is a wiring change, and this study is its go/no-go.
- **A live endpoint-key safety bug.** [RateLimitInterceptor.java](adaptive-rate-limiter/src/main/java/com/ratelimiter/adaptive_rate_limiter/interceptor/RateLimitInterceptor.java) passes the raw URI (`/api/payment`) as the endpoint key, but the config is keyed on bare names (`payment`). The lookup misses, so per-endpoint limits and degradation modes silently fall back to defaults. **This should be fixed regardless of the algorithm decision.**

---

## 7. Decision & rationale

**Classify each endpoint, then pick the strategy from its class — don't choose one universal default.**

```
Endpoint class
   ├── EXACT_QUOTA   → Sliding Window   (payment, sms)
   └── RATE / PACING → GCRA             (search, ai-inference)
```

- **EXACT_QUOTA** — the limit is a hard, auditable ceiling. Requires Sliding Window: GCRA's rolling-window count can exceed N by design (§5.3), so it must not guard an exact quota.
- **RATE / PACING** — the goal is to protect a downstream from sustained overload. GCRA wins: smooth pacing, continuous recovery, and — decisively — **~207× less Redis memory**, which is what makes it viable at high key-cardinality and high limits.
- **Performance is not a deciding factor** — the ~6–9% server-side throughput penalty is small, partly rig noise, and dwarfed by transport latency.
- This class-based rule is cleaner than a controller that "arbitrarily switches algorithms," and it feeds the **adaptive-controller roadmap** directly: the class is a static, declarable property of each endpoint.

---

## 8. Reproduce it

```bash
cd adaptive-rate-limiter
sh mvnw -Dtest=GcraVsSlidingWindowDecisionBenchmark -DfailIfNoTests=false \
        -Dsurefire.useFile=false test
```
Requires Docker (Testcontainers pulls `redis:7-alpine`). Tunables:
`-Dbench.trials`, `-Dbench.durationMs`, `-Dbench.warmupMs`, `-Dbench.concurrency`, `-Dbench.footprintKeys`.
The report prints between `===BENCH-REPORT-START===` and `===BENCH-REPORT-END===`.

---

## 9. Limitations & next steps

- **Latency is transport-bound** on the local named-pipe rig. For production-grade latency numbers, re-run against a **co-located Redis over a real network** — the memory and semantic conclusions already hold and won't change.
- **Single JVM, single Redis.** Cross-node clock behavior is handled (GCRA uses Redis `TIME`), but multi-node contention isn't modeled here.
- **Then act on it, in this order:**
  1. **Fix the endpoint-key bug** in the interceptor (independent of GCRA) so per-endpoint config is actually selected. *(Done — [EndpointKeyResolver.java](adaptive-rate-limiter/src/main/java/com/ratelimiter/adaptive_rate_limiter/interceptor/EndpointKeyResolver.java) + [EndpointKeyResolverTest.java](adaptive-rate-limiter/src/test/java/com/ratelimiter/adaptive_rate_limiter/interceptor/EndpointKeyResolverTest.java).)*
  2. **Add a GCRA semantic torture-test suite** *before wiring anything* — prove: idle burst = exactly N; the (N+1)-th immediate request is rejected; sustained rate ≈ N/W; **rolling-window count can exceed N (documented deliberately, §5.3)**; retry-after is correct; TAT never moves backward; a rejected request does not advance TAT; Redis `TIME` is used; concurrent callers cannot over-advance TAT; the key expires after the debt drains. *(Done — [GcraInvariantTest.java](adaptive-rate-limiter/src/test/java/com/ratelimiter/adaptive_rate_limiter/service/strategy/GcraInvariantTest.java): 12 invariants against real Redis covering all 10 properties. The rolling-window-exceeds-N property is proven empirically by `invariant_12` — a rolling 1 s window admits **more than** the limit, ≈ 2·limit − 1.)*
  3. **Add explicit per-endpoint strategy selection** — a policy lookup, not an `if GCRA … else …` toggle. *(Done — [StrategyType.java](adaptive-rate-limiter/src/main/java/com/ratelimiter/adaptive_rate_limiter/model/StrategyType.java) + a `strategy` field on each endpoint policy; `RateLimiterService.distributedStrategyFor(endpoint)` resolves it via `RateLimiterProperties.getStrategyForEndpoint`. Routed payment/sms → SLIDING_WINDOW, search/ai-inference → GCRA in [application.properties](adaptive-rate-limiter/src/main/resources/application.properties); window stays global for now. Verified by [RateLimiterServiceStrategySelectionTest.java](adaptive-rate-limiter/src/test/java/com/ratelimiter/adaptive_rate_limiter/service/RateLimiterServiceStrategySelectionTest.java). Token Bucket remains the local degraded-mode fast path.)*
  4. **Then build the adaptive layer** with distinct responsibilities, not four interchangeable algorithms: GCRA / Sliding Window = distributed quota allocator; Token Bucket = local fast-path / degraded admission; quota leasing = fewer Redis calls; adaptive controller = decides how much quota to lease and which policy applies.
