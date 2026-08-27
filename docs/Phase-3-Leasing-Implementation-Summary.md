# Phase 3 Quota Leasing — Implementation Summary

**Adaptive Distributed Rate Limiter · what was built and why**
_Spring Boot 4.1 · Java 21 · Redis 7 + atomic Lua · 2026-08-25_

---

## TL;DR — the money slide

> **Phase 3 adds a quota-leasing layer that takes the Redis round trip off the hot path for RATE/PACING endpoints — cutting Redis calls by ~99 % on healthy traffic — without changing the guarantee those endpoints make.**
> A pod **leases a batch of K quota units in one Redis call** (by advancing the shared GCRA TAT), then admits the next K-1 requests **locally**, against a depleting in-memory bucket. It was built strictly **correctness-first** in the order **3A → 3B → 3C → 3D → 3E**, with the adaptive controller **last**, so every guarantee was proven against a fixed K before adaptivity was introduced. **EXACT_QUOTA endpoints (payment, sms) never lease** — enforced in code, not config.

---

## 1. The problem

In Phase 2, **every healthy request costs one Redis `EVAL`.** That makes Redis the throughput ceiling and puts a network round trip on every request's hot path. But RATE/PACING endpoints (`search`, `ai-inference`) don't need per-request precision — their job is to shield a downstream from *sustained* overload, and GCRA already tolerates a bounded burst by design. So for those endpoints we can pay Redis far less often by **leasing a batch of quota** and admitting locally against it.

EXACT_QUOTA endpoints (`payment`, `sms`) are different: their limit is a hard, auditable ceiling. They **stay entirely on the Phase 2 per-request path** — leasing must never touch them.

---

## 2. The core idea — lease a batch, admit locally

```
Phase 2 (every request):        Phase 3 leased (RATE/PACING only):
  request ──► Redis EVAL           request ──► local bucket has tokens? ──► admit   (no Redis)
          ◄── allow/deny                            │ empty
                                                     ▼
                                          lease K units ──► Redis (one EVAL) ──► top up bucket
```

- **One Redis call per K admits** instead of one per admit.
- The lease reserves quota **by advancing the GCRA TAT** — the *same* key and TAT the direct GCRA path uses. This is the coordination mechanism: because all P pods advance one shared TAT, their combined output stays paced to the global limit. It is explicitly **not** a naive "Redis stores an integer, subtract K" counter (that would lose pacing and self-expiry).
- Unused units are **refunded on the next lease call** (folded in, no extra round trip), so trapped quota is bounded by the lease TTL rather than the full window.

---

## 3. What was built, phase by phase

Built in a strict, enforced order so all correctness is proven against a **fixed lease size K** before any adaptivity.

### Phase 3A — batch-GCRA allocator (fixed K, no adaptivity)
The allocator that reserves a batch by advancing the TAT.
- `lua/lease_quota.lua` — atomic batch reservation: `baseTat = max(oldTat − unused·emission, now); granted = min(requested, floor((now + dvt − baseTat) / emission))`; advances TAT by `granted · emission`; `PEXPIRE`s to the leased virtual time. Returns `{granted, newTat, now, nextAvailableMs}`.
- `model/LeaseGrant.java` — `record LeaseGrant(int granted, long leaseExpiryMs, long nextAvailableMs)`.
- `service/quota/QuotaAllocator.java` + `GcraQuotaAllocator.java` — the interface and its Redis-backed implementation, mirroring `GcraStrategy` exactly (same key `ratelimit:<key>:gcra`, deny-by-default fail-safe).
- **Key property:** `lease(key, 1, …)` reduces **exactly** to per-request GCRA. An idle key grants exactly `limit`.

### Phase 3B — LeaseManager + LocalQuotaBucket (dedicated depleting bucket)
The per-pod lease lifecycle.
- `service/quota/LocalQuotaBucket.java` — a per-key **depleting counter** topped up only by lease grants. It reuses `TokenBucketStrategy`'s Caffeine + `synchronized(bucket)` idiom but has **no time-based refill** — a continuously refilling local bucket would let P pods each admit at `limit/window` ≈ P× the global limit. Pacing lives in the allocator, not locally.
- `service/quota/LeaseManager.java` — fast path (local admit, no Redis) → async **prefetch** at a watermark so refills overlap serving → synchronous lease on an empty bucket → `null` degrade signal if Redis is down. New algorithm label `GCRA_LEASED`.
- Wired into `RateLimiterService` behind the **hard guard**: `leasing.enabled && getStrategyForEndpoint(endpoint) == GCRA`. `TokenBucketStrategy` is left untouched as the DEGRADED-mode strategy.

### Phase 3C — failure, degradation & refund
The graceful-degradation semantics — the highest-value correctness work, done **before** adaptivity.
- **Survive-then-degrade:** a held, valid lease keeps admitting after Redis dies (the fast path touches no Redis); only once it drains does the manager signal degrade, routing to the existing `checkDegraded` path. No raw Redis error ever reaches the request.
- **RECOVERY:** resume leasing at `minLease`, not full K, so a just-recovered Redis isn't hammered.
- **Refund:** `lease_quota.lua` credits `unusedFromLastLease` before granting; the manager passes unused tokens on renewal.

### Phase 3D — adaptive controller (LAST)
- `service/quota/AdaptiveLeaseController.java` — EWMA of consumption rate + **AIMD**: additive-increase under sustained load, multiplicative-decrease (×0.5) on idle expiry or a partial (contended) grant. Always clamped to `[minLease, maxLeaseFraction × limit]`.
- **Gated behind `rate-limiter.leasing.adaptive` (default false)** so the fixed-K correctness proofs remain the default behavior; the manager only calls the controller when the flag is on.

### Phase 3E — multi-pod correctness + benchmark + metrics
- `benchmark/MultiPodLeasingTest.java` — P independent pods over one shared Redis: single-pod call-collapse, multi-pod aggregate-rate coordination, and instantaneous-overshoot bound, plus an opt-in Phase 2 vs Phase 3 benchmark headlined "Redis calls saved."
- `metrics/RateLimiterMetrics.java` — added lease meters: `rate_limiter_lease_grants_total`, `rate_limiter_lease_redis_calls_saved_total`, `rate_limiter_lease_wasted_quota_total`, `rate_limiter_lease_starvation_total`, and a `lease_size` gauge.

---

## 4. Files at a glance

**Created (production):**
| File | Role |
|---|---|
| `resources/lua/lease_quota.lua` | Atomic batch-GCRA reservation + refund |
| `model/LeaseGrant.java` | Lease result record |
| `service/quota/QuotaAllocator.java` | Allocator interface |
| `service/quota/GcraQuotaAllocator.java` | Redis-backed batch allocator |
| `service/quota/LocalQuotaBucket.java` | Per-key depleting local bucket (no refill) |
| `service/quota/LeaseManager.java` | Lease lifecycle: fast path, prefetch, degrade |
| `service/quota/AdaptiveLeaseController.java` | EWMA + AIMD lease sizing (flag-gated) |

**Modified:**
| File | Change |
|---|---|
| `redis/LuaScriptLoader.java` | Load + expose `lease_quota.lua` |
| `config/RateLimiterProperties.java` | `Leasing` config block (enabled, minLease, maxLeaseFraction, prefetchWatermark, targetReleaseIntervalMs, leaseTtlMs, adaptive) |
| `resources/application.properties` | `rate-limiter.leasing.*` defaults (feature **off**) |
| `service/RateLimiterService.java` | Inject `LeaseManager`; leased path behind the GCRA guard; degrade fall-through |
| `metrics/RateLimiterMetrics.java` | Five lease meters + gauge |

**Untouched (deliberately):** `sliding_window.lua`, `SlidingWindowStrategy`, `TokenBucketStrategy` (stays the DEGRADED strategy), the EXACT_QUOTA request path, the health state machine / circuit breaker.

---

## 5. Key design decisions (the non-obvious ones)

| Decision | Why |
|---|---|
| **Reserve by advancing the TAT**, not decrementing a counter | Preserves GCRA pacing and key self-expiry; makes P pods coordinate through one shared TAT. A counter would be a distributed integer, not a rate limiter. |
| **Dedicated `LocalQuotaBucket` with no time-based refill** | A refilling local bucket would let each of P pods admit at the full rate → ~P× the global limit. The bucket only depletes; all refill comes from leases. |
| **Adaptive sizing gated behind a flag (default off)** | Correctness is proven against a fixed K first; a cold key in adaptive mode ramps from `minLease`, which would otherwise complicate the 3A–3C proofs. |
| **EXACT_QUOTA guard is the strategy taxonomy, not a new flag** | `getStrategyForEndpoint == GCRA` is already false for payment/sms → they can never enter the leased path. One source of truth. |
| **Refund folded into the next lease call** | Bounds trapped quota to the lease TTL with zero extra round trips. |
| **Degrade = return `null`, never throw** | Keeps raw Redis errors off the hot path; reuses the existing `checkDegraded` state machine. |

---

## 6. The guarantee — stated precisely

Leasing is applied **only** to RATE/PACING (GCRA) endpoints, and it preserves their guarantee class:

| Endpoint class | Guarantee |
|---|---|
| **EXACT_QUOTA** (payment, sms → Sliding Window) | Exact rolling-window ceiling: never more than N in any rolling W-second window. **Never leases.** |
| **RATE/PACING** (search, ai-inference → GCRA), leased | Long-run rate → N/W **+** a bounded, quantified instantaneous overshoot ≤ **P × maxLeaseFraction × limit** (per key) **+** trapped quota bounded by lease TTL. |

**Leasing does not, and is not meant to, provide an exact rolling-window ceiling for RATE/PACING endpoints.** It trades a precisely-bounded instantaneous overshoot for a ~99 % reduction in Redis calls. See the semantics table in [GCRA-vs-Sliding-Window-Decision.md](adaptive-rate-limiter/docs/GCRA-vs-Sliding-Window-Decision.md) and the design in [Phase-3-Adaptive-Quota-Design.md](adaptive-rate-limiter/docs/Phase-3-Adaptive-Quota-Design.md).

---

## 7. Result

**31 tests across 8 classes, 0 failures, 0 errors.** On healthy, well-distributed traffic the benchmark shows **99 % of Redis calls eliminated** (100,000 → 1,000) with identical admissions and p99 latency down from 11.8 ms to 2.2 ms; under single-hot-key abuse leasing correctly provides ~1 % benefit and leaks no extra admissions. Full test-by-test detail and the benchmark tables are in [Phase-3-Leasing-Test-Results.md](adaptive-rate-limiter/docs/Phase-3-Leasing-Test-Results.md).

**Status: Phase 3 (3A → 3E) complete and verified.**
