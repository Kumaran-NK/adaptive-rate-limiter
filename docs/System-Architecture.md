# Adaptive Self-Healing Rate Limiter — System Architecture

**What this system is, what it does today, and how the pieces fit together.**
_Spring Boot 4.1 · Java 21 · Redis 7 + atomic Lua · Resilience4j · Micrometer/Prometheus · 2026-08-24_

> Companion document: [GCRA-vs-Sliding-Window-Decision.md](GCRA-vs-Sliding-Window-Decision.md) — the benchmark and semantics study behind the per-endpoint algorithm choice. This document describes the *system as built*; that one explains *why* the algorithm routing is the way it is.

---

## 1. In one paragraph

This is a **distributed API rate limiter** that fronts every `/api/**` request, decides allow/deny against a shared Redis counter, and **keeps working when Redis degrades or fails**. It is "adaptive" in two senses: (1) each endpoint is routed to the rate-limiting *algorithm* that matches its semantics — exact-quota endpoints to **Sliding Window**, rate/pacing endpoints to **GCRA** — and (2) a background **self-healing state machine** continuously watches Redis health and shifts the whole limiter between four operating modes (HEALTHY → WARNING → DEGRADED → RECOVERY), trading exactness for availability only when it has to, and returning to full strength automatically once Redis recovers.

---

## 2. What it does today (capabilities)

- **Per-request rate limiting** on all `/api/**` routes via a Spring MVC interceptor, keyed on the caller's `X-API-Key` (falling back to remote IP).
- **Two distributed algorithms, chosen per endpoint by policy** (not a hardcoded toggle):
  - **Sliding Window** — exact rolling count, for hard/auditable quotas (`payment`, `sms`).
  - **GCRA** — smooth pacing at `limit/window`, ~207× less Redis memory, for throughput shaping (`search`, `ai-inference`).
- **A local Token Bucket fast-path** used only when Redis is unavailable (degraded mode) — purely in-memory, no Redis dependency.
- **Self-healing across four health states** with **hysteresis** (harder to enter a bad state than to leave it) and **stabilization windows** (a state must persist before a transition commits), so the system doesn't flap.
- **Circuit-breaker-protected Redis calls** (Resilience4j `"redis"` breaker) — a failing Redis trips the breaker instead of hanging every request.
- **Configurable degradation policy per endpoint** — `FAIL_OPEN`, `FAIL_CLOSED`, or `FAIL_STRICT` decide what happens to that endpoint's traffic when Redis is down.
- **Fleet-coordinated recovery** — a shared Redis `healthy-since` signal lets multiple pods agree on when the system has been healthy long enough to recover.
- **Alert suppression + flap detection** — distinguishes a real sustained outage from transient blips before alerting.
- **Full observability** — Prometheus metrics for allow/deny counts, request latency percentiles, current health state, and state-transition counts; per-response diagnostic headers; a health-state HTTP endpoint.

---

## 3. High-level architecture

The system has two planes that share Redis but run independently:

- **Data plane** (synchronous, per request): interceptor → service → strategy → Redis.
- **Control plane** (asynchronous, scheduled): health probe → state machine → circuit breaker + metrics + alerts. The control plane sets the *mode* the data plane runs in.

```
                         HTTP request  (/api/**)
                               │
                               ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ DATA PLANE  (per request, synchronous)                         │
   │                                                                │
   │  RateLimitInterceptor ── EndpointKeyResolver (URI → "payment") │
   │         │  key = X-API-Key or remote IP                        │
   │         ▼                                                      │
   │  RateLimiterService.isAllowed(key, endpoint)                   │
   │         │                                                      │
   │         ├── reads current HealthState ◄───────────────┐       │
   │         │                                              │       │
   │         ▼  dispatch by state                           │       │
   │   HEALTHY / RECOVERY → distributedStrategyFor(endpoint)│       │
   │        │                    │                          │       │
   │        │        ┌───────────┴───────────┐              │       │
   │        │        ▼                       ▼              │       │
   │        │  SlidingWindowStrategy    GcraStrategy        │       │
   │        │  (EXACT_QUOTA)            (RATE/PACING)        │       │
   │        │        └──── atomic EVAL ────┐                │       │
   │        │  wrapped in Resilience4j "redis" breaker      │       │
   │   WARNING → local cache w/ periodic Redis re-sync      │       │
   │   DEGRADED → TokenBucketStrategy (local) or deny       │       │
   │        │        per endpoint's DegradationMode         │       │
   │        ▼                                               │       │
   │   RateLimitDecision  → response headers, 429 if denied │       │
   └────────┼───────────────────────────────────────────────┼──────┘
            │                                               │
            ▼                                               │ current state
        ┌───────┐                              ┌────────────┴───────────┐
        │ Redis │◄──── PING (scheduled) ────────│ CONTROL PLANE          │
        │  7 +  │                              │                        │
        │  Lua  │  ratelimit:global:           │ RedisHealthProbe       │
        │       │  healthy-since (shared)      │   (p50/p95/p99, errors)│
        └───────┘◄─────────────────────────────│        │               │
                                               │        ▼               │
                                               │  StateMachine (4 states)│
                                               │        │               │
                                               │        ├─ HealthStateMetrics
                                               │        ├─ CircuitBreakerMonitor
                                               │        └─ AlertSuppressionService
                                               └────────────────────────┘
```

---

## 4. The request path (data plane), step by step

1. **`WebMvcConfig`** registers **`RateLimitInterceptor`** for `addPathPatterns("/api/**")` and `excludePathPatterns("/actuator/**")` — so business endpoints are limited, but metrics/health scraping is not.
2. **`RateLimitInterceptor.preHandle`**:
   - Derives the **client key**: `X-API-Key` header, or the remote IP if the header is absent.
   - Derives the **endpoint name** via **`EndpointKeyResolver.resolve(uri)`** — strips the query string and a leading `api/` segment and lower-cases the next segment, so `/api/payment` → `payment`, `/api/ai-inference` → `ai-inference`, `/api/payment/refund` → `payment`. (This resolver fixed a real bug where the raw URI was passed as the key, causing every per-endpoint config lookup to miss and silently fall back to global defaults.)
   - Calls **`RateLimiterService.isAllowed(key, endpoint)`**.
3. **`RateLimiterService.isAllowed`** reads the current **`HealthState`** from the `StateMachine` and dispatches:
   - **HEALTHY** → `checkWithCircuitBreaker` — run the endpoint's distributed strategy through the Redis circuit breaker; seed the local cache with the result.
   - **WARNING** → `checkWarning` — serve from the local cache (decrementing a cached remaining count) to shed load off Redis, but every **5th** call goes to Redis to re-sync; a cache miss also falls through to Redis.
   - **DEGRADED** → `checkDegraded` — every **10th** call probes Redis (only if the breaker isn't OPEN) to detect recovery; otherwise apply the endpoint's **degradation mode** (see §7).
   - **RECOVERY** → same as HEALTHY (full Redis path) — the state machine promotes to HEALTHY once it's stable.
4. **Algorithm selection** (`distributedStrategyFor`): `properties.getStrategyForEndpoint(endpoint)` returns `GCRA` or `SLIDING_WINDOW`; the service dispatches to `gcraStrategy` or `slidingWindowStrategy` accordingly. This is the **single place** the two distributed algorithms are chosen between — a data-driven policy lookup.
5. The strategy returns a **`RateLimitDecision`** (`allowed`, `remaining`, `resetTimeMillis`, `systemHealth`, `algorithmUsed`).
6. Back in the interceptor, four response headers are always set — `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `X-System-Health`, `X-Algorithm-Used` — and if denied, the response is **HTTP 429** with `{"error":"Rate limit exceeded"}`. Any thrown exception becomes **HTTP 503** `{"error":"Service degraded"}`.
7. **`RateLimiterMetrics`** records the allow/deny outcome for Prometheus.

---

## 5. The algorithms

All Redis decisions are **one atomic `EVAL`** (a single round trip); the difference is what each stores and what it guarantees.

| Strategy | Where it runs | Redis key | State stored | Guarantee | Memory | Used for |
|---|---|---|---|---|---|---|
| **Sliding Window** | Redis + `sliding_window.lua` | `ratelimit:<key>:window` (sorted set) | one member per admitted request | **exact** — never more than N in any rolling window | O(limit)/key (~25 KB at limit 200) | `payment`, `sms` (EXACT_QUOTA) |
| **GCRA** | Redis + `gcra.lua` | `ratelimit:<key>:gcra` (single scalar TAT) | one Theoretical Arrival Time value | **pacing** — sustained rate → limit/window; burst against idle key = limit; rolling count can transiently reach ~2·limit−1 by design | O(1)/key (~120 B) | `search`, `ai-inference` (RATE/PACING) |
| **Token Bucket** | **Local (in-memory Caffeine)** | none — key used directly as map key | tokens + last-refill per key | local approximation, no cross-pod coordination | in-JVM only | **degraded-mode fallback only** |

- **Sliding Window** (`sliding_window.lua`): `ZREMRANGEBYSCORE` drops expired entries, `ZCARD` counts live ones, and if under the limit it `ZADD`s this request and `PEXPIRE`s the key. Exact rolling count.
- **GCRA** (`gcra.lua`): reads Redis' own clock via `TIME` (so all pods agree on "now" with no clock skew), computes `emission_interval = period_ms / limit`, and admits if the TAT allows it, advancing the TAT on admit and self-expiring the key as the debt drains. With `DVT = period`, burst capacity against an idle key equals the limit. Its semantics are locked down by **`GcraInvariantTest`** (12 invariants against real Redis, including `invariant_12` which proves the rolling-window count can exceed the limit by design).
- **Token Bucket** (`TokenBucketStrategy`): a private Caffeine map of `TokenBucket` objects (`expireAfterAccess(5m)`, max 10 000), refilling `capacity / (windowSeconds·1000)` tokens per ms. Every decision is tagged `DEGRADED` / `TOKEN_BUCKET` — it is only ever reached when Redis is unavailable.

`SlidingWindowStrategy`, `GcraStrategy`, and `TokenBucketStrategy` all implement the same **`RateLimitStrategy`** interface (`isAllowed(key, limit, windowSeconds)` + `getAlgorithmName()`), which is what makes per-endpoint routing a clean lookup rather than a branch.

---

## 6. Per-endpoint policy

Each endpoint declares three independent dimensions in `application.properties` under `rate-limiter.endpoints.<name>`; unset values fall back to global defaults. **The window size is currently global** (`rate-limiter.window-size-seconds=60`) — only limit, strategy, and degradation mode are per-endpoint today.

| Endpoint | Limit | Strategy | Degradation mode | Class |
|---|--:|---|---|---|
| `payment` | 10 | `SLIDING_WINDOW` | `FAIL_CLOSED` | EXACT_QUOTA |
| `sms` | 5 | `SLIDING_WINDOW` | `FAIL_CLOSED` | EXACT_QUOTA |
| `ai-inference` | 50 | `GCRA` | `FAIL_STRICT` | RATE/PACING |
| `search` | 200 | `GCRA` | `FAIL_OPEN` | RATE/PACING |
| _(unconfigured)_ | 100 | `SLIDING_WINDOW` | `FAIL_STRICT` | default |

Resolution lives in **`RateLimiterProperties`**: `getStrategyForEndpoint(endpoint)` returns the endpoint's `strategy` or the global `default-strategy`; `getDegradationMode(endpoint)` and `getLimitForEndpoint(endpoint)` behave the same way. Routing is verified by **`RateLimiterServiceStrategySelectionTest`** (payment→Sliding Window, search→GCRA, unknown→default, all with mocked collaborators — no Redis).

---

## 7. Degradation modes — what happens when Redis is down

When the system is **DEGRADED** and the recovery probe hasn't succeeded, each endpoint's `DegradationMode` decides its fate:

| Mode | Behavior in DEGRADED | Intended for |
|---|---|---|
| **`FAIL_CLOSED`** | **Deny** the request (`DEGRADED_CLOSED`). Better to reject than to risk over-admitting. | Money/side-effect endpoints — `payment`, `sms`. |
| **`FAIL_STRICT`** | Admit via **local Token Bucket** with a **per-pod limit** = `max(totalLimit / podCount, 1)`, so the fleet's combined local admission stays near the global limit even without Redis coordination. | Expensive-but-not-exact endpoints — `ai-inference`, and the default. |
| **`FAIL_OPEN`** | Admit via **local Token Bucket** at the **full limit** per pod (favor availability). | Cheap, high-volume endpoints — `search`. |

`podCount` for `FAIL_STRICT` comes from **`PodDiscoveryService`**, which reads the `POD_COUNT` or `REPLICAS` env var (Kubernetes downward API), defaulting to 1.

---

## 8. The self-healing control plane

This is the "self-healing" half — it runs on a scheduler, independent of request traffic, and determines which mode §4 dispatches into.

### 8.1 Health probing — `RedisHealthProbe`

- `@Scheduled(fixedDelay=3000ms, initialDelay=5000ms)` (enabled by `@EnableScheduling` on the application class).
- `PING`s Redis, maintains a sliding window of the last **20** latencies + errors, computes **p50/p95/p99 and error rate**, and skips the first probe (warm-up).
- Writes/reads a **shared** `ratelimit:global:healthy-since` key (`setIfAbsent`, earliest-wins, TTL 120 s) so all pods share a fleet-wide notion of "healthy since when."
- Builds a **`HealthCheckEvent`** (percentiles, error rate, reachability, timestamp, shared healthy-since) and calls `StateMachine.evaluateHealth(...)`; on a transition it notifies `AlertSuppressionService`.

### 8.2 The state machine — `StateMachine`

Four states in `HealthState`, with **hysteresis** (distinct enter/exit thresholds) and **stabilization windows** (a condition must hold for N seconds before the transition commits):

```
                    circuit OPEN, or p99 > 200ms sustained
        ┌──────────────────────────────────────────────────────────┐
        │                                                            ▼
   ┌─────────┐  p99>50ms or errRate>1%   ┌─────────┐          ┌──────────┐
   │ HEALTHY │ ────────────────────────► │ WARNING │ ───────► │ DEGRADED │
   └─────────┘                           └─────────┘          └──────────┘
        ▲                                     │                     │
        │  stable & healthy                   │ p99<20ms &          │ Redis reachable,
        │  (recovery-stab 30s)                │ err=0 & stable      │ p99 ok, circuit
        │                                     │ (warning-stab 15s)  │ HALF_OPEN/CLOSED,
        │                                     ▼                     │ stable (degraded-stab 30s)
   ┌──────────┐   healthy & stable       ┌─────────┐               │
   │ RECOVERY │ ◄────────────────────────┤ HEALTHY │◄──────────────┘
   └──────────┘                          └─────────┘        (or forced recovery after
        │  p99>50ms or err>1% → WARNING                      degraded-stab + 60s cooldown
        └────────────────────────────────────────────►      via the shared healthy-since signal)
```

Key thresholds (from `application.properties`, all tunable):

| Parameter | Value | Meaning |
|---|--:|---|
| `enter-warning-latency-ms` | 50 | p99 above this (or error rate > 1%) leaves HEALTHY |
| `exit-warning-latency-ms` | 20 | p99 must drop below this (and error rate 0) to return to HEALTHY |
| `enter-healthy-latency-ms` | 10 | "healthy enough" ceiling used in recovery checks |
| `warning-stabilization-seconds` | 15 | dwell time before WARNING transitions commit |
| `degraded-stabilization-seconds` | 30 | dwell time before DEGRADED can move to RECOVERY |
| `recovery-stabilization-seconds` | 30 | dwell time before RECOVERY promotes to HEALTHY |

The lower *exit* threshold (20 ms) vs *enter* threshold (50 ms) is the hysteresis band that prevents oscillation around a single latency value. `isHealthyEnoughForRecovery` additionally requires Redis reachable, p99 ≤ max(enter-healthy, exit-warning), and error rate ≤ 1%. When the circuit is stuck OPEN, a **forced recovery** path (guarded by an extra 60 s + the shared healthy-since signal) prevents the system from getting permanently stuck in DEGRADED. Every transition updates `HealthStateMetrics` and returns a `StateTransition` record.

### 8.3 The circuit breaker — Resilience4j `"redis"`

- Defined in `Resilience4jConfig` as `registry.circuitBreaker("redis")`, with an `onStateTransition` logger; `CircuitBreakerMonitor` registers additional listeners (errors, calls-not-permitted).
- Config: sliding-window-size 10, failure-rate-threshold 50%, wait-in-open 30 s, permitted-in-half-open 3, minimum-calls 5, automatic OPEN→HALF_OPEN enabled.
- **Data plane:** every Redis strategy call in HEALTHY/RECOVERY runs through `circuitBreaker.executeSupplier(...)`; a blocked/failed call is caught and routed to `checkDegraded`.
- **Control plane:** the breaker's state (OPEN / HALF_OPEN / CLOSED) is one of the inputs the state machine uses to move between health states.

### 8.4 Alerting — `AlertSuppressionService`

Tracks recent transitions to avoid alert noise: it detects **flapping** (≥ 5 transitions within a 600 s window), alerts on a **sustained WARNING** only after 120 s, alerts once on **DEGRADED**, and resets/clears everything on return to **HEALTHY**. `StateTransitionLogger` provides structured transition logging.

---

## 9. Component map (packages → responsibility)

| Package | Key types | Responsibility |
|---|---|---|
| `interceptor` | `RateLimitInterceptor`, `EndpointKeyResolver` | Entry point; extract key + endpoint, set headers, 429/503. |
| `service` | `RateLimiterService`, `PodDiscoveryService` | Orchestration: state dispatch, strategy selection, degradation; pod count. |
| `service.strategy` | `RateLimitStrategy`, `SlidingWindowStrategy`, `GcraStrategy`, `TokenBucketStrategy` | The three algorithms behind one interface. |
| `service.state` | `StateMachine`, `AlertSuppressionService`, `StateTransitionLogger` | Health state transitions + alerting. |
| `service.health` | `RedisHealthProbe`, `CircuitBreakerMonitor` | Scheduled probing + breaker event logging. |
| `redis` | `LuaScriptLoader` | Loads `sliding_window.lua` and `gcra.lua` at startup. |
| `cache` | `LocalRateLimitCache` | Caffeine-backed local counts for WARNING mode. |
| `config` | `RedisConfig`, `CacheConfig`, `Resilience4jConfig`, `WebMvcConfig`, `RateLimiterProperties` | Beans + typed configuration. |
| `metrics` | `RateLimiterMetrics`, `HealthStateMetrics` | Micrometer/Prometheus instrumentation. |
| `model` | `HealthState`, `HealthCheckEvent`, `RateLimitDecision`, `StateTransition`, `DegradationMode`, `StrategyType` | Enums + records shared across layers. |
| `controller` | `HealthStateController`, `RateLimitTestController` | `/api/health/state` + synthetic demo endpoints. |
| `exception` | `GlobalExceptionHandler`, `RedisExceptionHandler`, `RateLimitExceededException`, `RedisUnavailableException` | Map failures to 429 / 503 JSON. |
| `resources/lua` | `gcra.lua`, `sliding_window.lua` | The atomic decision scripts. |

> **Note on legacy code:** `service/RedisRateCheckService` exists but is **not wired into any live path** (no bean injects it) — the production path is `RateLimitInterceptor → RateLimiterService`. It's an earlier alternate implementation kept in the tree.

---

## 10. Observability

- **Metrics** (Prometheus via `/actuator/prometheus`):
  - `rate_limiter_requests_total{decision=allowed|denied}` — allow/deny counters.
  - `rate_limiter_request_duration` — request latency Timer (p50/p95/p99).
  - `rate_limiter_health_state` — gauge (HEALTHY=0, WARNING=1, DEGRADED=2, RECOVERY=3).
  - `rate_limiter_state_transitions_total{from,to}` — transition counter.
- **Response headers** on every `/api/**` request: `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `X-System-Health`, `X-Algorithm-Used`.
- **HTTP:** `GET /api/health/state` returns the current state name, ordinal code, and timestamp. Actuator exposes `health`, `info`, `prometheus`, `metrics`.

---

## 11. How it's tested

| Test | What it proves | Redis? |
|---|---|---|
| `GcraInvariantTest` | 12 GCRA semantic invariants (burst=N, (N+1)th rejected, sustained≈N/W, rolling count can exceed N by design, retry-after correctness, TAT monotonicity, uses Redis `TIME`, key expiry, concurrency safety). | real (Testcontainers) |
| `RateLimiterServiceStrategySelectionTest` | Per-endpoint routing: payment→Sliding Window, search→GCRA, unknown→default. | mocked |
| `GcraVsSlidingWindowDecisionBenchmark` | Throughput/latency/memory comparison behind the algorithm decision. | real |
| `StateMachineTest` | Health-state transition logic. | — |
| `SelfHealingChaosTest` | End-to-end degradation/recovery under induced Redis failure. | real |
| `RateLimiterIntegrationTest` | Full request path through the interceptor. | real |

Run against real Redis requires Docker (Testcontainers pulls `redis:7-alpine`):

```bash
cd adaptive-rate-limiter
sh mvnw test
```

---

## 12. Status & roadmap

- **Phase 1 — algorithm study & correctness (Done).** Benchmark + 12-invariant GCRA torture-test suite; the endpoint-key bug fixed via `EndpointKeyResolver`.
- **Phase 2 — per-endpoint strategy selection (Done).** `StrategyType` + policy lookup (`distributedStrategyFor`), `payment`/`sms` → Sliding Window, `search`/`ai-inference` → GCRA. Window stays global; Token Bucket remains the local degraded path.
- **Phase 3 — adaptive quota layer (Roadmap).** Distinct responsibilities rather than interchangeable algorithms: GCRA / Sliding Window as the distributed quota allocator, Token Bucket as the local fast-path, **quota leasing** to cut Redis round trips, and an **adaptive controller** that decides how much quota each pod leases and which policy applies under load.

---

_See [GCRA-vs-Sliding-Window-Decision.md](GCRA-vs-Sliding-Window-Decision.md) for the data and reasoning behind the algorithm routing described in §5–§6._
