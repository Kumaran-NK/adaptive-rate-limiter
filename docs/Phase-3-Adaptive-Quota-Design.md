# Phase 3 — Adaptive Quota Layer (Design)

**How the rate limiter stops asking Redis on every request — without giving up its guarantees.**
_Design spec · builds on Phase 1 (algorithm study) + Phase 2 (per-endpoint strategy) · 2026-08-24_

> Companion docs: [System-Architecture.md](System-Architecture.md) (what's built today) · [GCRA-vs-Sliding-Window-Decision.md](GCRA-vs-Sliding-Window-Decision.md) (why the algorithm routing). This document is a **design**, not shipped code — it defines the components, the protocol, the tradeoff, and the rollout for the adaptive quota layer.

---

## TL;DR — the money slide

> **Phase 3 replaces "one Redis round trip per request" with "one Redis round trip per _lease_ of K requests."** Each pod leases a batch of quota from Redis, admits locally against it (Token Bucket, zero Redis calls), and re-leases before it runs dry. An **adaptive controller** sizes each lease to live demand.
>
> **This is only applied to RATE/PACING endpoints (`search`, `ai-inference`). EXACT_QUOTA endpoints (`payment`, `sms`) keep the Phase 2 per-request path — because leasing trades exactness for efficiency, and exactness is the entire reason those endpoints exist.**

| Axis | Phase 2 (today) | Phase 3 (RATE/PACING) |
|---|---|---|
| Redis calls | 1 per request | 1 per **K** requests (K adapts, typ. 10–200×fewer) |
| Hot-path latency | Redis RTT every request | local (sub-µs) between leases |
| Global limit enforcement | exact (per request) | **approximate, bounded** — sustained rate still converges to `limit` |
| Redis as bottleneck | throughput ceiling = Redis ops/s | ceiling lifts ~K× |
| EXACT_QUOTA endpoints | unchanged | **unchanged** (deliberately not leased) |

The single deciding constraint below drives every other decision.

---

## 1. The problem Phase 3 solves

In Phase 2, every HEALTHY request executes one atomic `EVAL` against Redis. That's correct and exact, but it makes **Redis the throughput ceiling and a per-request latency tax**:

- Every admitted request pays a Redis round trip on the hot path.
- Total system throughput is bounded by Redis ops/sec, regardless of how many pods you add.
- A popular key funnels all its traffic through a single Redis key/shard.

For endpoints where an *exact* count matters, that tax is the price of correctness and we pay it. For endpoints that only need to **shield a downstream from sustained overload**, paying it on every request is waste — those endpoints can tolerate an *approximate* global limit, and approximation is cheap.

---

## 2. The core idea: quota leasing

Change the question the pod asks Redis:

```
Phase 2:  "May I admit this 1 request?"          → 1 EVAL per request
Phase 3:  "May I lease K units for the next T?"  → 1 EVAL per K requests
```

Redis (the **allocator**) atomically decrements the global budget by the granted amount and returns a **lease**. The pod seeds a **local Token Bucket** with those K tokens and admits requests locally — **no Redis call** — until the tokens run low or the lease nears expiry, at which point it **re-leases asynchronously, ahead of exhaustion**, so the hot path never blocks on Redis.

Redis load drops by ~K×. The hot path becomes a local token check.

---

## 3. THE constraint: leasing is only safe for RATE/PACING

This is the design's linchpin, and it flows straight from the project's existing taxonomy.

When a pod holds a lease and admits locally, **no other pod — and not Redis — sees those admissions until the pod reports back.** At any instant the true global in-flight count is the *sum of all outstanding leases*, which is coarser than a per-request check. That coarseness is fine for some endpoints and fatal for others:

| Endpoint class | Can it be leased? | Why |
|---|---|---|
| **EXACT_QUOTA** — `payment`, `sms` | **No** | The limit is a hard, auditable ceiling. Leasing lets the global admitted count drift above N within a window (each pod admits "in advance" of central accounting). Unacceptable — so these **keep the Phase 2 direct per-request Sliding Window path.** They pay the Redis round trip *because that round trip is what buys exactness.* |
| **RATE/PACING** — `search`, `ai-inference` | **Yes** | The goal is to protect a downstream from sustained overload; small transient overshoot is already tolerated (GCRA admits ~2·limit in a rolling window *by design* — see the decision doc §5.3). Leasing's approximation sits inside that existing tolerance. |

So Phase 3 leasing is **selective**, gated by the same `StrategyType` / endpoint class that Phase 2 already resolves per endpoint. The routing table becomes a clean 2×2:

```
                       Direct per-request           Leased local
                       (Phase 2 path)               (Phase 3 path)
   EXACT_QUOTA    ┌──────────────────────────┬──────────────────────────┐
   payment, sms   │   ✅  stays here          │   ❌  breaks exactness    │
                  ├──────────────────────────┼──────────────────────────┤
   RATE/PACING    │   current                │   ✅  moves here          │
   search, ai-inf │                          │                          │
                  └──────────────────────────┴──────────────────────────┘
```

This is the same principle as Phase 2: *move forward selectively, not as a universal replacement.*

---

## 4. The efficiency ↔ accuracy tradeoff (stated honestly)

Lease size **is** the accuracy dial. There is no free lunch, and the design is explicit about the bound rather than hiding it:

- **Bigger leases → fewer Redis calls (efficiency ↑), but two accuracy costs:**
  1. **Undershoot / trapped quota.** A pod that leases K then goes idle "traps" that quota; other pods can't use it until the lease expires. Aggregate throughput can dip *below* `limit`.
  2. **Burst overshoot.** More pods holding tokens simultaneously means a larger instantaneous burst above the paced rate.
- **Smaller leases → tighter enforcement, but more Redis calls** (approaches Phase 2 as K→1).

**What bounds it:**

- **Sustained rate is still hard-bounded by `limit`.** The allocator's global budget refills at exactly `limit/window`; a pod can only lease what the allocator grants. Over any sustained interval, total admits converge to `limit` — leasing changes the *smoothness and instantaneous distribution*, not the long-run rate.
- **Worst-case instantaneous overshoot** ≤ `P × maxLeaseTokens` above the paced rate, where `P` = pod count and `maxLeaseTokens = maxLeaseFraction × limit`. Capping `maxLeaseFraction` (e.g. 0.25) bounds it, and no single pod can ever lease the whole budget (fairness).
- **Trapped quota** is bounded by `(idle leases) × leaseTtl`; unused tokens are **refunded on renewal**, and the TTL caps how long a *crashed* pod's quota is lost.

**Adaptivity is what keeps this honest** (§6): shrink leases when demand is low (accuracy matters, small leases are affordable at low traffic) and grow them under load (efficiency matters, and high aggregate traffic statistically smooths per-pod coarseness).

---

## 5. Components

Phase 3 adds four pieces and **reuses** the existing Token Bucket and GCRA machinery rather than replacing them.

```
   RATE/PACING request (search, ai-inference)
        │
        ▼
   LeaseManager.tryAdmit(key)
        │  local token check — no Redis on the hot path
        ├── token available ──────────────► ADMIT (local)
        │
        ├── tokens < watermark ──► async prefetch ──┐
        │                                           ▼
        └── tokens == 0 ──► (rare) block/deny   QuotaAllocator.lease(key, K, limit, window)
                                                    │  1 atomic EVAL: lease_quota.lua
                                                    ▼
                                              ┌──────────┐
                                              │  Redis   │  batch-GCRA over ratelimit:<key>:gcra
                                              └──────────┘
                                                    ▲
                            AdaptiveLeaseController sets K from observed demand
```

### 5.1 `QuotaAllocator` (distributed, Redis + Lua) — the global budget owner
- New script **`lease_quota.lua`**. Operation: *"grant up to `requested` units for `key` over `window`; return `{granted, leaseExpiryMs, nextAvailableMs}`."*
- **Implemented as batch-GCRA** — the natural fit. Per-request GCRA advances the TAT by one `emission_interval`; the allocator advances it by `granted × emission_interval`, granting as many units as fit before the TAT would exceed `now + DVT`. GCRA is *already* a scalar-per-key pacer, so leasing is just "advance the TAT by a batch." **No new algorithm and no new key** — RATE/PACING endpoints are already on GCRA (`ratelimit:<key>:gcra`); leasing extends it.
- Sliding Window is *not* used as a lease allocator (leasing K discrete sorted-set members is exactly the O(limit) memory cost GCRA avoids — see decision doc §5.2). SW stays on the direct EXACT_QUOTA path only.

### 5.2 `LocalQuotaBucket` (local fast-path) — reuses `TokenBucketStrategy`
- The existing Token Bucket, but its tokens are **sourced from leases** instead of a fixed per-pod refill rate. Admits locally against granted tokens.
- This **unifies Phase 3 with the Phase 2 degraded path**: same local-bucket mechanism, the token *source* just switches — "lease from Redis" when healthy, "fixed per-pod rate" when degraded.

### 5.3 `LeaseManager` (per pod, per key)
- Holds current lease state, admits from `LocalQuotaBucket`, and **prefetches the next lease at a low watermark** (e.g. 20% tokens remaining) so re-leasing overlaps with serving and never stalls the hot path.
- On acquisition failure (Redis down / circuit breaker OPEN): keep serving from the current lease until it expires, then fall to the endpoint's `DegradationMode` — exactly the Phase 2 behavior.

### 5.4 `AdaptiveLeaseController` — sizes each lease to demand
- Tracks an EWMA of recent consumption rate per key and targets a **re-lease interval** (e.g. every ~2 s): `K ← clamp(consumptionRate × targetReleaseInterval, minLease, maxLeaseFraction × limit)`.
- **AIMD-style correction:** additively grow K when leases are consumed fully and quickly (under-provisioned); multiplicatively shrink when leases expire with tokens unused (over-provisioned) or the allocator returns partial grants (global contention).
- Bounds guarantee fairness (no pod hoards the budget) and cap the overshoot term from §4.

---

## 6. Integration with the self-healing state machine

Phase 3 **composes with** Phase 2's four-state machine; it does not replace it. Per health state, for a RATE/PACING endpoint:

| State | Behavior |
|---|---|
| **HEALTHY** | Leased local path (RATE/PACING). EXACT_QUOTA still direct per-request. |
| **WARNING** | Already served — WARNING's goal is "reduce Redis load," which leasing does *automatically* (fewer calls). The controller can simply stretch the re-lease interval. |
| **DEGRADED** | Lease acquisition fails (breaker OPEN) → keep admitting from the last valid lease until it expires, then fall to `FAIL_OPEN`/`FAIL_STRICT`/`FAIL_CLOSED` as today. **Leasing makes DEGRADED *more* graceful:** a pod that already holds a lease when Redis dies keeps admitting *correctly* for the lease's remaining life before any fallback. |
| **RECOVERY** | Resume leasing conservatively — start at `minLease` and let the adaptive controller ramp back up, so a just-recovered Redis isn't hammered by full-size lease requests. |

The pleasing property: **leasing degrades into the existing Token Bucket path along the same seam** — one local-bucket mechanism, two token sources. Phase 2 and Phase 3 become one coherent model rather than two subsystems.

---

## 7. New / changed surface (interfaces, not implementation)

```java
// Redis
lua/lease_quota.lua                 // atomic batch grant over the GCRA TAT

// Allocator
interface QuotaAllocator {
    LeaseGrant lease(String key, int requested, int limit, int windowSeconds);
}
record LeaseGrant(int granted, long leaseExpiryMs, long nextAvailableMs) {}
class GcraQuotaAllocator implements QuotaAllocator { /* runs lease_quota.lua */ }

// Local + control
class LeaseManager          { boolean tryAdmit(String key); }   // prefetch + fallback
class AdaptiveLeaseController{ int nextLeaseSize(String key); }  // EWMA + AIMD

// Wiring
RateLimiterService.checkWithCircuitBreaker(...)
    → RATE/PACING: LeaseManager.tryAdmit(key)     // leased path
    → EXACT_QUOTA: distributedStrategyFor(...)     // unchanged direct path
```

New `RateLimiterProperties` (all per-endpoint-overridable, default **off**):

| Property | Default | Meaning |
|---|--:|---|
| `leasing.enabled` | `false` | Feature flag; enable per endpoint. |
| `leasing.min-lease` | 1 | Floor on K (K=1 ≡ Phase 2 behavior). |
| `leasing.max-lease-fraction` | 0.25 | Cap on K as a fraction of `limit` (fairness + overshoot bound). |
| `leasing.prefetch-watermark` | 0.2 | Re-lease when remaining tokens drop below this fraction. |
| `leasing.target-release-interval-ms` | 2000 | Controller's target time between leases. |
| `leasing.lease-ttl-ms` | 5000 | Lease lifetime; bounds trapped quota on crash. |

---

## 8. Observability (new metrics)

- `rate_limiter_lease_grants_total{key}` and a `lease_size` distribution summary.
- `rate_limiter_lease_redis_calls_saved_total` — estimated Redis calls avoided (= admits − lease grants).
- `rate_limiter_lease_wasted_quota_total` — tokens refunded unused (the undershoot cost from §4).
- `rate_limiter_lease_starvation_total` — hot-path admits that had to wait for a lease (should be ~0 if prefetch is tuned).
- `rate_limiter_lease_size` gauge per key — lets you *see* the adaptive controller working.

---

## 9. Testing & rollout

**Rollout:** `leasing.enabled` defaults off; canary on **`search` only** (FAIL_OPEN, most tolerant), then `ai-inference`. EXACT_QUOTA endpoints are never eligible — enforced in code, not just config.

**Test plan:**
- **Multi-pod simulation harness** — `P` threads, each with its own `LeaseManager`, sharing one real Redis (Testcontainers), extending the existing benchmark rig.
- **Invariants to prove:**
  1. Sustained global admits across P pods ≤ `limit` within the stated band (rate converges).
  2. Instantaneous overshoot ≤ `P × maxLeaseFraction × limit` (the §4 bound holds).
  3. Unused lease tokens are refunded; trapped quota ≤ `leaseTtl` after a pod goes idle.
  4. **EXACT_QUOTA endpoints are never routed to leasing** (regression guard on the §3 constraint).
  5. DEGRADED falls back correctly when the breaker opens mid-lease; a held lease keeps admitting until expiry.
  6. Adaptive sizing grows K under sustained load and shrinks it when idle.
- **Redis-call reduction** measured end-to-end (the headline efficiency claim) with the calls-saved metric.

---

## 10. Risks & non-goals

- **Not a linearizable global limit — by design.** Phase 3 is *approximate* global enforcement, applied only where approximation is already acceptable (RATE/PACING). EXACT_QUOTA correctness is untouched.
- **Clocks:** lease *grant/expiry* accounting uses Redis `TIME` (the hard bound); local token expiry uses the local clock. Skew only affects *when a pod re-leases*, never the global rate bound (Redis remains the source of truth).
- **Fairness is best-effort** (first-come-first-served leasing + per-pod cap). Proportional-share allocation across pods is a possible future extension, not in this design.
- **Tuning sensitivity:** a badly tuned prefetch watermark can cause starvation (too low) or waste (too high); the `starvation`/`wasted_quota` metrics exist to tune it.

---

## 11. Why this is the right shape

- It **honors the taxonomy the whole project is built on** — leasing is gated by EXACT_QUOTA vs RATE/PACING, the same axis as Phase 2's strategy selection.
- It **reuses, doesn't reinvent** — batch-GCRA is the allocator, Token Bucket is the local bucket, the state machine and degradation modes are unchanged. The four roadmap responsibilities land exactly as stated in [System-Architecture.md](System-Architecture.md) §12: GCRA = distributed quota allocator, Token Bucket = local fast-path/degraded admission, quota leasing = fewer Redis calls, adaptive controller = how much to lease.
- It is **honest about the tradeoff** — the accuracy band is quantified and bounded, not hand-waved, and it is only accepted where the semantics already tolerate it.
