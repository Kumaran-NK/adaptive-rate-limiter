# Phase 3 Quota Leasing — Test Results

**Adaptive Distributed Rate Limiter · quota-leasing verification report**
_Real Redis (Testcontainers `redis:7-alpine`) · JUnit 5 + Testcontainers + Mockito · 2026-08-25_

---

## TL;DR — the money slide

> **The quota-leasing layer is fully verified: 31 tests across 8 classes, 0 failures, 0 errors.**
> On healthy, well-distributed traffic, leasing serves the **same number of admissions for 1 % of the Redis calls** — a **99 % reduction** (100,000 → 1,000) — while p99 latency drops from **11.8 ms to 2.2 ms**. Under single-hot-key abuse it correctly falls back to near-per-request cost (1 % saved) and leaks **no** extra admissions. Every correctness invariant is phrased to the guarantee the endpoint class actually makes — long-run rate + bounded, quantified overshoot — never a false "≤ limit per rolling window" claim.

| Question the tests answer | Verified? | Evidence |
|---|---|---|
| Does a batch lease reduce **exactly** to per-request GCRA at K=1? | ✅ | `GcraQuotaAllocatorTest.leaseOfOne_isEquivalentToDirectGcra` |
| Does one lease of K serve K admits at **one** Redis call? | ✅ | `LeaseManagerTest` (100 admits → 10 calls) |
| Do P pods **coordinate** through the shared TAT (not P× the limit)? | ✅ | `MultiPodLeasingTest` Test 2 |
| Is instantaneous overshoot bounded by **P × maxLeaseFraction × limit**? | ✅ | `MultiPodLeasingTest` Test 3 |
| Does a held lease **survive Redis death**, then degrade cleanly? | ✅ | `LeaseFailureTest`, `LeaseManagerTest` Test 5 |
| Can EXACT_QUOTA (payment/sms) **ever** lease? | ✅ Never | `RateLimiterServiceLeasingGuardTest` |
| How many Redis calls does leasing actually save? | ✅ 99 % / 1 % | Phase 2 vs Phase 3 benchmark (below) |

---

## 1. Suite at a glance

All counts read directly from `target/surefire-reports/TEST-*.xml` (the `-q` Maven flag suppresses the console summary, so the XML is the source of truth).

| Phase | Test class | Backing | Tests | Fail | Err | Skip |
|---|---|---|--:|--:|--:|--:|
| 3A | `GcraQuotaAllocatorTest` | Real Redis | 6 | 0 | 0 | 0 |
| 3B/C/D | `LeaseManagerTest` | Fake allocator (in-memory) | 7 | 0 | 0 | 0 |
| 3C | `LeaseFailureTest` | Real Redis (killed mid-run) | 2 | 0 | 0 | 0 |
| 3C | `LeaseRefundTest` | Real Redis | 2 | 0 | 0 | 0 |
| 3C | `RateLimiterServiceLeasingGuardTest` | Mocked collaborators | 3 | 0 | 0 | 0 |
| 3D | `AdaptiveLeaseControllerTest` | Pure unit (fake clock) | 4 | 0 | 0 | 0 |
| 3E | `MultiPodLeasingTest` | Real Redis (P pods) | 3 (+1)¹ | 0 | 0 | 0 |
| — | `RateLimiterServiceStrategySelectionTest` | Mocked collaborators | 3 | 0 | 0 | 0 |
| | **Total** | | **31** | **0** | **0** | **0** |

¹ `MultiPodLeasingTest` has 3 always-on correctness invariants plus 1 opt-in benchmark. In a normal `mvn test` the benchmark is **skipped** (shows `skipped=1`); with `-Dbench.multipod=true` all 4 run (`skipped=0`). The count above is from the benchmark run.

**Test-design note.** The tests deliberately mix backings: the allocator↔Redis batch-GCRA arithmetic is proven once against **real Redis** (`GcraQuotaAllocatorTest`, `LeaseRefundTest`); `LeaseManagerTest` then isolates the local bucket + lease bookkeeping with a **fake allocator** (no Redis) so its counts are deterministic; `LeaseFailureTest` uses a **dedicated, killable** container to prove real Lettuce failure handling; `MultiPodLeasingTest` runs **P independent pods**, each with its own connection, against **one shared** Redis.

---

## 2. Phase 3A — batch-GCRA allocator (real Redis)

The allocator reserves quota **by advancing the GCRA TAT**, not by decrementing a counter — that is the coordination mechanism. These six prove the arithmetic against real Redis (limit 10 / window 10 s / emission 1000 ms).

| # | Test | Asserts |
|---|---|---|
| 1 | `leaseOfOne_isEquivalentToDirectGcra` | A lease of size 1 grants **iff** a direct `GcraStrategy.isAllowed` admits — in lockstep across idle → burst → saturated → denied. **K=1 reduces exactly to Phase 2.** |
| 2 | `idleBatch_grantsExactlyLimit_thenNothing` | An idle key grants a full burst of **exactly `limit`**; the immediate next lease grants **0**. |
| 3 | `partialLease_advancesTatByGrantedEmissionIntervals` | `lease(4)` grants 4 and advances the TAT by **exactly 4 emission intervals**; a second `lease(4)` advances it 4 more. |
| 4 | `overAsk_neverGrantsMoreThanLimit` | Asking for `5 × limit` against an idle key grants **exactly `limit`, never more**; then 0. |
| 5 | `tatMonotonic_andNextAvailableSignalsSaturation` | TAT is **strictly monotonic**; `nextAvailableMs == 0` while slack remains, `> 0` once saturated. |
| 6 | `leasedKey_selfExpiresAfterVirtualTimeDrains` | Like `gcra.lua`, the key is `PEXPIRE`'d to its leased virtual time and **disappears** once that drains (no leak). |

---

## 3. Phase 3B/3C/3D — LeaseManager (fake allocator, deterministic)

Isolates the local bucket + lease bookkeeping. Config tuned for a clean K: `limit = 100`, `maxLeaseFraction = 0.1` → **K = 10**.

| # | Test | Asserts |
|---|---|---|
| 1 | `servesKAdmitsFromASingleLease` | K admits cost **exactly one** lease (one Redis call); the other K-1 are served locally with **no Redis**. |
| 2 | `redisCallsEqualAdmitsDividedByK` | 100 admits collapse into **exactly 10** leases (`admits / K`). |
| 3 | `prefetchesAtWatermarkAheadOfExhaustion` | The async prefetch fires at the watermark (remaining ≤ 2) so the refill **overlaps serving**; only the initial empty-bucket lease is synchronous. |
| 4 | `returnsNullToSignalDegradeWhenLeaseUnavailableAndBucketEmpty` | Empty bucket + Redis down → returns **`null`** (degrade signal), never throws a raw Redis error onto the request path. |
| 5 | `heldLeaseKeepsServingAfterRedisDies_thenSignalsDegradeOnExhaustion` | A held lease keeps serving K-1 admits after Redis dies (**touches no Redis**); only once drained does it signal degrade. |
| 6 | `recoveryModeLeasesAtMinLeaseNotFullK` | In RECOVERY it leases only **`minLease`** (eases a just-recovered Redis back in); HEALTHY leases the full K. |
| 7 | `adaptiveModeSizesLeaseViaControllerNotFixedK` | With `leasing.adaptive = true`, a cold key leases **`minLease`** via the controller — proof the adaptive path is wired in and bypasses the fixed K. |

---

## 4. Phase 3C — failure, degradation & refund

### 4.1 `LeaseFailureTest` — real Redis, killed mid-run (2)
| # | Test | Asserts |
|---|---|---|
| 1 | `heldLeaseKeepsServingAfterRedisDies_thenDegradesOnExhaustion` | Draw a lease from a **live** Redis → **stop the container** → the K-1 held tokens keep serving (no raw error reaches the request) → drained + still down → **`null`** degrade signal. |
| 2 | `coldBucketWithRedisDownSignalsDegradeNotError` | Redis down **before** any lease → `null`, never a propagated Redis exception. |

### 4.2 `LeaseRefundTest` — real Redis (2)
| # | Test | Asserts |
|---|---|---|
| 1 | `refundCreditsExactlyUnusedUnitsBackToThePool` | Refunding 22 unused units on renewal returns **exactly 22** units of capacity to the shared TAT vs a no-refund control — this bounds trapped quota to the lease lifetime, not the whole window. |
| 2 | `refundIsClampedAndNeverExceedsLimitOnAnIdleKey` | An over-refund is **clamped to "now"** — a refund can never push the shared clock backwards or manufacture capacity beyond `limit`. |

### 4.3 `RateLimiterServiceLeasingGuardTest` — the hard EXACT_QUOTA guard (mocked, 3)
| # | Test | Asserts |
|---|---|---|
| 1 | `exactQuotaEndpointNeverLeasesEvenWhenLeasingEnabled` | With `leasing.enabled = true`, a `payment` (SLIDING_WINDOW) request **never** reaches `LeaseManager` — verified with `verify(leaseManager, never())`; it takes the direct Sliding-Window path. |
| 2 | `ratePacingEndpointIsServedByLeaseManager` | A `search` (GCRA) request **is** served by `LeaseManager`; `gcra.isAllowed` is never called; the decision carries the `GCRA_LEASED` label. |
| 3 | `leaseDegradeSignalFallsThroughToDegradedTokenBucket` | When `LeaseManager` returns `null`, the service degrades to the **Token Bucket** (FAIL_OPEN) with `DEGRADED` health — no raw error. |

> **This is the guard that keeps payment/sms exact.** It is the existing strategy taxonomy (`getStrategyForEndpoint == GCRA`), not a separate flag, so EXACT_QUOTA endpoints are structurally excluded from leasing.

---

## 5. Phase 3D — adaptive controller (pure unit, fake clock)

Deterministic via an injected clock — no sleeps. Config `limit = 100`, `minLease = 1`, `maxLeaseFraction = 0.25` → **maxK = 25**, `targetReleaseIntervalMs = 2000`.

| # | Test | Asserts |
|---|---|---|
| 1 | `growsUnderSustainedLoadUpToMaxK` | Under sustained load (leases drained faster than target), K climbs **monotonically** to the `maxLeaseFraction × limit` ceiling and stops. |
| 2 | `shrinksWhenIdleDownToMinLease` | When idle (long gaps between leases), K backs off **multiplicatively** down to `minLease` and floors there. |
| 3 | `partialGrantDrivesKDownFullGrantDoesNot` | A partial (contended) grant triggers **×0.5** multiplicative backoff; a full grant leaves K **unchanged**. |
| 4 | `neverLeavesBoundsAcrossMixedLoad` | Across an arbitrary mix of fast load, idle gaps and a contended grant, every size handed out — and the internal estimate — stays within **[minLease, maxK]**. |

> Adaptive sizing is gated behind `rate-limiter.leasing.adaptive` (default **false**), so the 3A–3C fixed-K correctness proofs remain the default behavior.

---

## 6. Phase 3E — multi-pod correctness invariants (real Redis, P pods)

Each pod is a fully independent `LeaseManager` with its own connection; all share one Redis, so they coordinate only through the shared GCRA TAT. **Observed values below are from the 2026-08-25 run.**

| # | Test | Config | Asserts | Observed |
|---|---|---|---|---|
| 1 | `singlePodCollapsesRedisCallsToAdmitsOverK` | limit 1000, K=10, 1000 req | All 1000 admit at **`requests/K = 100`** leases; every admit is either the one that drew a lease or a local admit; ≥ 90 % of Redis calls saved. | `admitted=1000 redisCalls=100 saved=900 (90.0%)` — **deterministic** |
| 2 | `multiPodSustainedAggregateRateConvergesToLimit` | 5 pods, 100/1 s, 3 s | Aggregate **sustains ~limit rate** and is **far below** the P× rate uncoordinated limiters would emit (coordination proof). Explicitly **not** "≤ limit per rolling window." | `admits=393 over 3.0s → 131/s` (target 100/s); well under the P×limit ceiling — timing-dependent, asserted as a band |
| 3 | `instantaneousOvershootStaysWithinPTimesMaxLeaseFractionLimit` | 5 pods, K=25 (P×K=125 > limit=100), 2 s | Peak admits in any 10 ms window ≤ **P × maxLeaseFraction × limit** (+ small slack). **Deliberately does NOT assert ≤ limit.** | `bound(P*K)=125 observedInstantMax=100 (exceeds limit? false)` |

> **Why Test 2 and Test 3 are phrased this way.** GCRA guarantees a long-run rate plus a bounded burst — never an exact rolling-window count. Leasing loosens the instant further, in a *quantified* way (each of P pods holds ≤ K unspent units per key). Asserting "≤ limit per rolling window" here would test a guarantee the system is designed **not** to make. See [Phase-3-Adaptive-Quota-Design.md](adaptive-rate-limiter/docs/Phase-3-Adaptive-Quota-Design.md) and the semantics table in [GCRA-vs-Sliding-Window-Decision.md](adaptive-rate-limiter/docs/GCRA-vs-Sliding-Window-Decision.md).

---

## 7. Phase 2 vs Phase 3 benchmark — the headline

Opt-in (`-Dbench.multipod=true`). **10 pods, 100,000 requests, limit 1000/60 s, K = 100.** Phase 2 = direct GCRA (one `EVAL`/request); Phase 3 = leased GCRA (Redis calls = sync + prefetch leases). Shared `redis:7-alpine`, each pod its own connection. Run of 2026-08-25.

### 7.1 HEALTHY — sub-limit, 100 distinct keys

| phase | requests | admitted | denied | **redis_calls** | p50 ms | p95 ms | p99 ms |
|---|--:|--:|--:|--:|--:|--:|--:|
| PHASE2 direct | 100,000 | 100,000 | 0 | **100,000** | 2.642 | 5.833 | 11.762 |
| PHASE3 leased | 100,000 | 100,000 | 0 | **1,000** | 0.000 | 0.013 | 2.152 |

> **Redis calls saved: 99,000 of 100,000 (99.0 %).** Identical admissions (100,000 = 100,000) — leasing changes cost, not correctness. p50 rounds to **0.000 ms** because the local admit touches no Redis; p99 improves **5.5×** (11.8 → 2.2 ms).
> Aggregate peak admits/10 ms across 100 keys = 9,926 (aggregate bound `numKeys × P × K` = 100,000; per-key bound P×K = 1,000).

### 7.2 ABUSE — over-limit, 1 hot key

| phase | requests | admitted | denied | **redis_calls** | p50 ms | p95 ms | p99 ms |
|---|--:|--:|--:|--:|--:|--:|--:|
| PHASE2 direct | 100,000 | 1,446 | 98,554 | **100,000** | 2.311 | 4.921 | 9.077 |
| PHASE3 leased | 100,000 | 1,419 | 98,581 | **99,010** | 2.227 | 4.274 | 7.775 |

> **Redis calls saved: 990 of 100,000 (1.0 %).** A single hot key hammered far past its budget **can't be batched** — most requests are denials, and a denial still costs a Redis probe in both phases. Admissions stay in lockstep (1,446 vs 1,419) — **leasing leaks no extra capacity under abuse.** Instantaneous overshoot on the hot key = 600 admits/10 ms, within the per-key bound P×K = 1,000.

> **The two scenarios together are the argument:** leasing pays off exactly when traffic is spread (the common, healthy case) and safely declines to help — without weakening the limit — when a single key is being abused.

---

## 8. Reproduce it

```bash
cd adaptive-rate-limiter

# All 31 leasing tests (correctness invariants; benchmark skipped)
sh mvnw -Dtest='GcraQuotaAllocatorTest,LeaseManagerTest,LeaseFailureTest,LeaseRefundTest,\
AdaptiveLeaseControllerTest,MultiPodLeasingTest,RateLimiterServiceLeasingGuardTest,\
RateLimiterServiceStrategySelectionTest' -DfailIfNoTests=false -Dsurefire.useFile=false test

# The Phase 2 vs Phase 3 benchmark (prints between ===BENCH-REPORT-START/END===)
sh mvnw -Dtest=MultiPodLeasingTest -DfailIfNoTests=false -Dsurefire.useFile=false \
        -Dbench.multipod=true test
```

Requires Docker (Testcontainers pulls `redis:7-alpine`). Benchmark tunables: `-Dbench.pods`, `-Dbench.requests`, `-Dbench.limit`, `-Dbench.window`, `-Dbench.maxLeaseFraction`. Confirm green by reading `target/surefire-reports/TEST-*.xml` (`tests`/`failures`/`errors`/`skipped` + absence of `<failure>`/`<error>`).

---

## 9. What these results do — and don't — prove

- **Do:** the batch-GCRA reservation is arithmetically correct (3A); one lease serves K admits at one Redis call (3B); held leases survive Redis death and degrade cleanly, refunds are exact, EXACT_QUOTA never leases (3C); adaptive K stays bounded (3D); P pods coordinate through the shared TAT with instantaneous overshoot bounded by P × maxLeaseFraction × limit (3E); and leasing saves ~99 % of Redis calls on healthy traffic.
- **Don't:** claim an exact rolling-window ceiling for GCRA/leased endpoints — that is Sliding Window's job for EXACT_QUOTA endpoints, and no leasing test asserts it. Latency is measured on a local named-pipe Docker rig, so absolute per-op numbers are transport-bound; the **Redis-call-count** reduction (the headline) is transport-independent and robust.
