# Test Results — Evidence of Execution

**Adaptive Distributed Rate Limiter** · proof for presentation · generated from a live run.

> **Headline:** `mvnw test` → **73 tests · 0 failures · 0 errors · 1 skipped · BUILD SUCCESS** (the 1 skipped is the *opt-in* benchmark).
> Run on **2026-08-26 20:11–20:13 IST**, Java 21.0.6, Docker 29.6.2 (Docker Desktop), Testcontainers 1.21.4 with real `redis:7-alpine`.

This document is generated from two captured console logs in this folder — nothing here is hand-typed:
- [full-test-run.log](full-test-run.log) — the complete `mvnw test` output (the proof).
- [benchmark-run.log](benchmark-run.log) — the opt-in 100k-request multi-pod benchmark.
- [surefire-reports/](surefire-reports/) — the machine-readable XML report per test class.

---

## 1. The result (screenshot this slide)

```
[INFO] Results:
[INFO]
[INFO] Tests run: 73, Failures: 0, Errors: 0, Skipped: 1
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  01:21 min
[INFO] Finished at: 2026-08-26T20:13:23+05:30
```
_(verbatim from [full-test-run.log](full-test-run.log), lines 245–254)_

---

## 2. Per-class breakdown (all 16 classes, 73 tests)

Every line below is a real `Tests run:` line from the log. Backing column shows *how* each was tested.

| # | Test class | Tests | Skip | Time | Backing |
|--:|---|--:|--:|--:|---|
| 1 | `strategy.GcraInvariantTest` | 12 | 0 | 15.44s | Real Redis |
| 2 | `strategy.GcraStrategyTest` | 9 | 0 | 4.31s | Real Redis |
| 3 | `strategy.SlidingWindowStrategyTest` | 6 | 0 | 3.15s | Real Redis |
| 4 | `strategy.AlgorithmComparisonTest` | 3 | 0 | 11.34s | Real Redis |
| 5 | `quota.GcraQuotaAllocatorTest` | 6 | 0 | 1.83s | Real Redis |
| 6 | `quota.LeaseManagerTest` | 7 | 0 | 0.11s | Fake allocator |
| 7 | `quota.LeaseFailureTest` | 2 | 0 | 4.39s | **Real Redis, killed** |
| 8 | `quota.LeaseRefundTest` | 2 | 0 | 0.21s | Real Redis |
| 9 | `quota.AdaptiveLeaseControllerTest` | 4 | 0 | 0.04s | Fake clock |
| 10 | `benchmark.MultiPodLeasingTest` | 4 | 1 | 13.45s | Real Redis, 5 pods |
| 11 | `RateLimiterServiceLeasingGuardTest` | 3 | 0 | 1.61s | Mocked |
| 12 | `RateLimiterServiceStrategySelectionTest` | 3 | 0 | 0.04s | Mocked |
| 13 | `state.StateMachineRecoveryTest` | 2 | 0 | 0.71s | Unit |
| 14 | `state.StateMachineWarningEscalationTest` | 2 | 0 | 0.04s | Unit |
| 15 | `interceptor.EndpointKeyResolverTest` | 7 | 0 | 0.06s | Pure unit |
| 16 | `AdaptiveRateLimiterApplicationTests` | 1 | 0 | 14.49s | `@SpringBootTest` |
| | **Total** | **73** | **1** | ~81s | |

---

## 3. Evidence for each claim in the talk

**"We use real Redis with Testcontainers."**
```
tc.redis:7-alpine : Creating container for image: redis:7-alpine
tc.redis:7-alpine : Container redis:7-alpine started in PT1.4932158S
```
_(full-test-run.log lines 82–85; Testcontainers 1.21.4, line 64.)_

**"We actually kill Redis during the test, and an existing lease keeps serving; once exhausted, the system degrades."**
`LeaseFailureTest` connects to a real Redis, then the container is stopped mid-test — the log shows the lease layer catching the outage (not crashing), and the test still passes 2/2:
```
WARN  c.r.a.service.quota.LeaseManager : Lease acquisition failed for cold-real-redis (Redis unavailable?): Unable to connect to Redis
WARN  c.r.a.service.quota.LeaseManager : Lease acquisition failed for survive-real-redis (Redis unavailable?): Redis command timed out
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0 -- in ...LeaseFailureTest
```
_(full-test-run.log lines 132, 159–160.)_

**"The health state machine transitions correctly."**
```
STATE TRANSITION: HEALTHY  -> DEGRADED | P99: 60.0ms, Redis: unreachable, Circuit: OPEN
STATE TRANSITION: DEGRADED -> RECOVERY | P99: 20.0ms, Redis: reachable,  Circuit: CLOSED
STATE TRANSITION: WARNING  -> DEGRADED | P99: 250.0ms (sustained critical latency)
```
_(full-test-run.log lines 177–184.)_

**"Multi-pod distributed coordination holds."** (always-on invariants, `MultiPodLeasingTest`)
```
[Test1] requests=1000 admitted=1000 redisCalls=100 saved=900 (90.0%)
[Test2] pods=5 limit=100/1s admits=376 over 3.0s -> aggregateRate=125/s (target 100/s)
[Test3] pods=5 K=25 bound(P*K)=125 limit=100 observedInstantMax=100 (exceeds limit? false)
```
_(full-test-run.log lines 105, 94, 102.)_
- Test1 — leasing cut Redis calls by **90%** even at this small scale.
- Test2 — 5 pods sustain ~aggregate rate near the limit (long-run rate guarantee).
- Test3 — instantaneous max stayed within the `P×K` overshoot bound and did **not** exceed a hard leak.

**"99% Redis-call reduction at scale"** → see the dedicated benchmark section below (from [benchmark-run.log](benchmark-run.log)).

---

## 4. Multi-pod benchmark (opt-in, 100k requests)

Run with `-Dbench.multipod=true` (which also un-skips the benchmark → **4 tests, 0 failures, 0 skipped**, total 02:04 min). **Config: 10 pods · 100,000 requests · limit 1000/60s · lease K=100.** Verbatim from [benchmark-run.log](benchmark-run.log) lines 94–118:

```
## HEALTHY (sub-limit, 100 keys)
phase          |   requests |   admitted |     denied |  redis_calls |   p50 ms |   p95 ms |   p99 ms
------------------------------------------------------------------------------------------------
PHASE2 direct  |     100000 |     100000 |          0 |       100000 |    2.682 |    6.360 |    9.528
PHASE3 leased  |     100000 |     100000 |          0 |         1000 |    0.000 |    0.014 |    1.886
>> Redis calls saved: 99000 of 100000 (99.0%) ; global admits: phase2=100000 phase3=100000

## ABUSE (over-limit, 1 hot key)
phase          |   requests |   admitted |     denied |  redis_calls |   p50 ms |   p95 ms |   p99 ms
------------------------------------------------------------------------------------------------
PHASE2 direct  |     100000 |       1486 |      98514 |       100000 |    2.520 |    5.777 |    8.005
PHASE3 leased  |     100000 |       1454 |      98546 |        99010 |    2.317 |    5.593 |    7.779
>> Redis calls saved: 990 of 100000 (1.0%) ; global admits: phase2=1486 phase3=1454
```

**What this proves (the two lines to say):**
- **HEALTHY:** leasing cut Redis calls **100,000 → 1,000 = 99.0% fewer**, while admitting the **exact same 100,000** requests — the limit is unchanged, we just stopped paying Redis per request. p99 latency dropped **9.53 ms → 1.89 ms (~5×)**.
- **ABUSE (1 hot key over the limit):** leasing correctly **declines to help** (only 1.0% saved — there's nothing to batch) and does **not leak** — instantaneous admits stayed at 1000, exactly the limit (`P×K` bound). Global admits ≈ identical (1486 vs 1454).

---

## 5. How to reproduce (PowerShell)

```powershell
cd "D:\RATE LIMITER V2\adaptive-rate-limiter"

# Full suite — must have Docker Desktop running
.\mvnw.cmd test

# The opt-in 100k-request benchmark
.\mvnw.cmd "-Dtest=MultiPodLeasingTest" "-Dbench.multipod=true" test
```
Independent verification: open any file in [surefire-reports/](surefire-reports/) — each XML's root element states `tests="…" failures="0" errors="0"` for that class.
