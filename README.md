# Adaptive Self-Healing Rate Limiter

A distributed, self-healing rate limiter built with Spring Boot 4.1 and Redis that automatically detects backend degradation, transitions through health states, switches rate-limiting algorithms on the fly, and recovers — all without human intervention.

Deployed on Kubernetes with HPA auto-scaling, monitored with Prometheus + Grafana, and chaos-tested with LitmusChaos.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Rate Limiting Algorithms](#rate-limiting-algorithms)
- [Health State Machine](#health-state-machine)
- [Degradation Modes](#degradation-modes)
- [Quota Leasing](#quota-leasing)
- [Per-Endpoint Policy](#per-endpoint-policy)
- [Observability](#observability)
- [Chaos Engineering](#chaos-engineering)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Testing](#testing)

---

## How It Works

Every incoming HTTP request passes through a `RateLimitInterceptor` that resolves the caller's identity (via `X-API-Key` header or IP address) and the target endpoint, then delegates to `RateLimiterService`. The service checks the current health state and picks the appropriate rate-limiting path:

1. **HEALTHY** — Distributed check via Redis (Sliding Window or GCRA), wrapped in a Resilience4j circuit breaker.
2. **WARNING** — Serves most requests from a local Caffeine cache; every 5th request still hits Redis to keep the cache warm.
3. **DEGRADED** — Redis is unreachable. Falls back to a local in-memory Token Bucket. The fallback behavior varies per endpoint (FAIL_OPEN, FAIL_CLOSED, or FAIL_STRICT).
4. **RECOVERY** — Redis is back. Gradually shifts traffic back to distributed checks with conservative lease sizes.

A background `RedisHealthProbe` pings Redis every 3 seconds, collects latency percentiles (P50/P95/P99) and error rates over a sliding window of 20 samples, and feeds them to the `StateMachine` which decides whether to transition states.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌─────────────────────── rate-limiter ns ─────────────────────┐ │
│  │                                                             │ │
│  │  ┌──────────────────┐     ┌──────────────────┐             │ │
│  │  │  Rate Limiter    │────▶│     Redis         │             │ │
│  │  │  Pod (×2, HPA)   │     │  (Lua Scripts)    │             │ │
│  │  │                  │     └──────────────────┘             │ │
│  │  │  • Interceptor   │                                      │ │
│  │  │  • StateMachine  │                                      │ │
│  │  │  • CircuitBreaker│                                      │ │
│  │  │  • HealthProbe   │                                      │ │
│  │  │  • LeaseManager  │                                      │ │
│  │  └──────────────────┘                                      │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌─────────────────────── monitoring ns ───────────────────────┐ │
│  │  ┌────────────┐     ┌────────────┐                         │ │
│  │  │ Prometheus │────▶│  Grafana   │                         │ │
│  │  │ (scrape)   │     │ (dashboard)│                         │ │
│  │  └────────────┘     └────────────┘                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌─────────────────────── litmus ns ──────────────────────────┐  │
│  │  LitmusChaos 2.14.0 — 8 chaos experiments                 │  │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## Rate Limiting Algorithms

Three algorithms are implemented. The system selects between them based on endpoint policy and health state:

| Algorithm | Implementation | Runs On | Used When |
|-----------|---------------|---------|-----------|
| **Sliding Window** | Lua script on Redis (`sliding_window.lua`). Adds a timestamped member per request to a sorted set, removes expired entries, and counts atomically. | Redis | HEALTHY/WARNING/RECOVERY state for EXACT_QUOTA endpoints (payment, sms) |
| **GCRA** | Lua script on Redis (`gcra.lua`). Maintains a single Theoretical Arrival Time (TAT) per key. O(1) memory. Uses Redis `TIME` command for clock — no app-server clock dependency. | Redis | HEALTHY/WARNING/RECOVERY state for RATE/PACING endpoints (search, ai-inference) |
| **Token Bucket** | In-memory via Caffeine cache. Refills tokens proportionally to elapsed time. | Local JVM | DEGRADED state (Redis unavailable) |

All Redis-side decisions use server timestamps from `redis.call('TIME')` — no application clock is passed, eliminating clock-skew issues across pods.

---

## Health State Machine

The `StateMachine` evaluates health on every probe (every 3s) and transitions states using hysteresis thresholds to prevent flapping:

```
                    ┌──────────────────────────────────┐
                    │                                  │
         ┌─────────▼─────────┐                        │
         │      HEALTHY      │                        │
         │  (Redis, full)    │                        │
         └────────┬──────────┘                        │
                  │ P99 > 50ms or                     │
                  │ error rate > 1%                    │
         ┌────────▼──────────┐                        │
         │      WARNING      │    stable + latency    │
         │  (cached + Redis) │────below threshold─────┘
         └────────┬──────────┘
                  │ circuit breaker OPEN
                  │ or sustained critical latency
         ┌────────▼──────────┐
         │     DEGRADED      │
         │  (local fallback) │
         └────────┬──────────┘
                  │ Redis reachable +
                  │ circuit HALF_OPEN/CLOSED +
                  │ stable for 30s
         ┌────────▼──────────┐
         │     RECOVERY      │
         │  (gradual ramp)   │──── relapse ──▶ WARNING
         └───────────────────┘
```

**Key details:**
- **Hysteresis** — Entry and exit thresholds are different (e.g., enter WARNING at P99 > 50ms, exit at P99 < 20ms) to avoid oscillation.
- **Stabilization windows** — Each state must be stable for a configurable duration before transitions are allowed (15s for WARNING, 30s for DEGRADED/RECOVERY).
- **Shared recovery signal** — A Redis key (`ratelimit:global:healthy-since`) coordinates recovery across pods using SETNX semantics, so all instances measure recovery stability from the same wall-clock moment.
- **Alert suppression** — Tracks state transitions over a sliding window and suppresses redundant alerts. Detects flapping (>5 transitions in 600s).

---

## Degradation Modes

When Redis is unavailable, the behavior is configured per endpoint:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `FAIL_OPEN` | Allow requests using local Token Bucket with the full configured limit | Read-heavy endpoints (search) |
| `FAIL_CLOSED` | Deny all requests immediately (HTTP 429) | Critical endpoints where over-admission is dangerous (payment, sms) |
| `FAIL_STRICT` | Allow requests using Token Bucket with limit divided by pod count (`limit / pods`) | Default — conservative but available |

Pod count for `FAIL_STRICT` is discovered via `PodDiscoveryService` using the `POD_COUNT` environment variable.

---

## Quota Leasing

An optional optimization for GCRA endpoints that batches Redis round trips:

- A `LeaseManager` acquires a batch of quota units from Redis in a single call (`lease_quota.lua`), then serves subsequent requests locally from a `LocalQuotaBucket` until the lease is exhausted or expires.
- An `AdaptiveLeaseController` uses EWMA (Exponentially Weighted Moving Average) to dynamically adjust lease sizes based on observed demand.
- **EXACT_QUOTA endpoints (payment, sms) are never leased** — the `StrategyType.GCRA` guard ensures only RATE/PACING endpoints enter the leased path.
- During RECOVERY state, leases are conservative (minimum size) to avoid overwhelming a just-recovered Redis.

**Metrics exposed:**
- `rate_limiter_lease_grants_total` — successful leases
- `rate_limiter_lease_redis_calls_saved_total` — round trips avoided
- `rate_limiter_lease_wasted_quota_total` — units forfeited on lease expiry (over-provisioning cost)
- `rate_limiter_lease_starvation_total` — requests that had to block on a synchronous lease

Disabled by default. Enable with `rate-limiter.leasing.enabled=true`.

---

## Per-Endpoint Policy

Each endpoint can be independently configured with its own limit, degradation mode, and distributed strategy:

```properties
# EXACT_QUOTA endpoints — hard ceiling, Sliding Window, deny on failure
rate-limiter.endpoints.payment.limit=10
rate-limiter.endpoints.payment.strategy=SLIDING_WINDOW
rate-limiter.endpoints.payment.degradation-mode=FAIL_CLOSED

rate-limiter.endpoints.sms.limit=5
rate-limiter.endpoints.sms.strategy=SLIDING_WINDOW
rate-limiter.endpoints.sms.degradation-mode=FAIL_CLOSED

# RATE/PACING endpoints — burst-tolerant, GCRA, allow on failure
rate-limiter.endpoints.search.limit=200
rate-limiter.endpoints.search.strategy=GCRA
rate-limiter.endpoints.search.degradation-mode=FAIL_OPEN

rate-limiter.endpoints.ai-inference.limit=50
rate-limiter.endpoints.ai-inference.strategy=GCRA
rate-limiter.endpoints.ai-inference.degradation-mode=FAIL_STRICT
```

Unconfigured endpoints fall back to global defaults (100 req/60s, Sliding Window, FAIL_STRICT).

---

## Observability

### Prometheus Metrics

The application exposes metrics at `/actuator/prometheus` via Micrometer:

| Metric | Type | Description |
|--------|------|-------------|
| `rate_limiter_requests_total` | Counter | Total requests, tagged by `decision` (allowed/denied) |
| `rate_limiter_request_duration` | Timer | Request processing latency with p50, p95, p99 percentiles |
| `rate_limiter_health_state` | Gauge | Current health state (0=HEALTHY, 1=WARNING, 2=DEGRADED, 3=RECOVERY) |
| `rate_limiter_state_transitions_total` | Counter | State transitions, tagged by `from` and `to` states |
| `rate_limiter_lease_grants_total` | Counter | Quota leases granted |
| `rate_limiter_lease_redis_calls_saved_total` | Counter | Redis round trips saved by leasing |
| `rate_limiter_lease_wasted_quota_total` | Counter | Leased quota units that expired unused |
| `rate_limiter_lease_starvation_total` | Counter | Requests that blocked waiting for a synchronous lease |
| `rate_limiter_lease_size` | Gauge | Size of the most recent quota lease |

Plus standard JVM, Resilience4j circuit breaker, and Spring Boot Actuator metrics.

### Grafana Dashboard

A pre-built dashboard ("Adaptive Rate Limiter") is auto-provisioned with 12 panels:

- **Header row** — Health state, allowed/denied rates, active pod count, circuit breaker state
- **Throughput** — Allowed vs denied request rates over time
- **Latency** — P50/P95/P99 percentile trends
- **State machine** — State transitions (bar chart) and health state timeline (step chart)
- **Quota leasing** — Grant rate, starvation events, Redis calls saved, wasted quota, lease size
- **Infrastructure** — JVM heap usage and Resilience4j circuit breaker call rates / failure rate

### Response Headers

Every rate-limited response includes:

```
X-RateLimit-Remaining: 42
X-RateLimit-Reset: 1757500000000
X-System-Health: HEALTHY
X-Algorithm-Used: SLIDING_WINDOW
```

---

## Chaos Engineering

8 LitmusChaos experiments target the `rate-limiter` namespace to validate resilience:

| # | Experiment | What It Does | Result |
|---|-----------|--------------|--------|
| 1 | Pod Delete | Gracefully deletes rate-limiter pods every 10s for 30s | ✅ Pass (100% probe success) |
| 2 | Pod CPU Hog | Injects 100% CPU load on 1 core for 60s | ⚙️ Configured |
| 3 | Pod Network Loss | Drops 100% of packets for 60s | ⚙️ Configured |
| 4 | Pod Network Latency | Injects 2000ms network latency for 60s | ⚙️ Configured |
| 5 | Redis Failure | Terminates the Redis pod | ✅ Pass (100% probe success) |
| 6 | Pod Memory Hog | Consumes 400Mi RAM (under 512Mi limit) for 60s | ⚙️ Configured |
| 7 | Pod Kill (Forced) | SIGKILL with grace period 0 | ✅ Pass (100% probe success) |
| 8 | Combined Failure | Multi-stage: pod delete + Redis failure + network loss | ⚙️ Configured |

Experiments 2–4, 6, and 8 are fully configured but require kernel-level access (`tc-netem`, cgroup) that local KIND clusters don't provide; they are ready for cloud Kubernetes (EKS/GKE/AKS).

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 21 |
| Framework | Spring Boot 4.1.0 |
| Distributed Store | Redis 7 with Lua scripting (3 scripts: sliding window, GCRA, lease quota) |
| Local Fallback | Caffeine cache (Token Bucket) |
| Resilience | Resilience4j Circuit Breaker |
| Observability | Micrometer → Prometheus v2.53.0 → Grafana 11.1.0 |
| Container | Docker (eclipse-temurin:21-jre-alpine) |
| Orchestration | Kubernetes (KIND) with HPA auto-scaling (2–5 replicas, 70% CPU target) |
| Chaos Testing | LitmusChaos 2.14.0 (8 experiments) |
| Build | Maven with Spring Boot plugin |
| Testing | JUnit 5, Testcontainers, 23 test classes |

---

## Project Structure

```
adaptive-rate-limiter/
├── src/main/java/.../
│   ├── controller/
│   │   ├── RateLimitTestController.java   # Test endpoints (/api/test, /api/payment, etc.)
│   │   └── HealthStateController.java     # GET /api/health/state
│   ├── interceptor/
│   │   ├── RateLimitInterceptor.java      # HTTP interceptor — rate limit enforcement
│   │   └── EndpointKeyResolver.java       # Maps URIs to endpoint policy names
│   ├── service/
│   │   ├── RateLimiterService.java        # Core orchestrator — state-aware routing
│   │   ├── RedisRateCheckService.java     # Redis rate check helper
│   │   ├── PodDiscoveryService.java       # Pod count for FAIL_STRICT mode
│   │   ├── state/
│   │   │   ├── StateMachine.java          # 4-state health state machine
│   │   │   ├── AlertSuppressionService.java # Flap detection + alert suppression
│   │   │   └── StateTransitionLogger.java
│   │   ├── strategy/
│   │   │   ├── RateLimitStrategy.java     # Strategy interface
│   │   │   ├── SlidingWindowStrategy.java # Redis Lua — exact quota
│   │   │   ├── GcraStrategy.java          # Redis Lua — rate pacing
│   │   │   └── TokenBucketStrategy.java   # Local fallback (Caffeine)
│   │   ├── quota/
│   │   │   ├── LeaseManager.java          # Batch quota leasing
│   │   │   ├── LocalQuotaBucket.java      # Per-key depleting counter
│   │   │   ├── QuotaAllocator.java        # Sliding Window lease allocator
│   │   │   ├── GcraQuotaAllocator.java    # GCRA lease allocator
│   │   │   └── AdaptiveLeaseController.java # EWMA-based lease sizing
│   │   └── health/
│   │       ├── RedisHealthProbe.java      # Scheduled probe (P50/P95/P99)
│   │       └── CircuitBreakerMonitor.java
│   ├── cache/
│   │   └── LocalRateLimitCache.java       # Caffeine-backed local cache
│   ├── redis/
│   │   └── LuaScriptLoader.java           # Loads Lua scripts from classpath
│   ├── metrics/
│   │   ├── RateLimiterMetrics.java        # Counters, timers, gauges
│   │   └── HealthStateMetrics.java        # State gauge + transition counter
│   ├── model/
│   │   ├── HealthState.java               # HEALTHY | WARNING | DEGRADED | RECOVERY
│   │   ├── DegradationMode.java           # FAIL_OPEN | FAIL_CLOSED | FAIL_STRICT
│   │   ├── StrategyType.java              # SLIDING_WINDOW | GCRA
│   │   ├── RateLimitDecision.java
│   │   ├── LeaseGrant.java
│   │   └── ...
│   └── config/
│       ├── RateLimiterProperties.java     # All configurable properties
│       ├── RedisConfig.java
│       ├── Resilience4jConfig.java
│       └── CacheConfig.java
├── src/main/resources/
│   ├── application.properties
│   └── lua/
│       ├── sliding_window.lua             # Atomic sorted-set rate limiter
│       ├── gcra.lua                       # Generic Cell Rate Algorithm
│       └── lease_quota.lua                # Batch quota allocation
├── src/test/                              # 23 test classes
├── k8s/
│   ├── namespace.yaml
│   ├── deployment.yaml                    # 2 replicas, probes, resource limits
│   ├── redis-deployment.yaml
│   ├── hpa.yaml                           # 2–5 replicas, 70% CPU target
│   └── monitoring/
│       ├── namespace.yaml
│       ├── prometheus-rbac.yaml
│       ├── prometheus-config.yaml         # Annotation-based pod auto-discovery
│       ├── prometheus-deployment.yaml
│       ├── grafana-datasource.yaml
│       ├── grafana-dashboard.yaml         # Pre-built 12-panel dashboard
│       └── grafana-deployment.yaml
├── litmus/
│   ├── 00-rbac.yaml
│   ├── 01-pod-delete.yaml
│   ├── 02-pod-cpu-hog.yaml
│   ├── 03-pod-network-loss.yaml
│   ├── 04-pod-network-latency.yaml
│   ├── 05-redis-failure.yaml
│   ├── 06-pod-memory-hog.yaml
│   ├── 07-pod-kill.yaml
│   ├── 08-combined-failure.yaml
│   └── chaos-test-results.md
├── docs/                                  # Architecture docs, design decisions
├── Dockerfile
├── docker-compose.yml
└── pom.xml
```

---

## Getting Started

### Prerequisites

- Java 21
- Docker Desktop
- Maven (or use the included `mvnw` wrapper)

### Run Locally with Docker Compose

```bash
# Build the application
./mvnw clean package -DskipTests

# Build the Docker image
docker build -t adaptive-rate-limiter:1.0.0 .

# Start all services (app, Redis, Prometheus, Grafana)
docker-compose up
```

| Service | URL |
|---------|-----|
| Rate Limiter API | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

### Deploy to Kubernetes (KIND)

```bash
# Create KIND cluster
kind create cluster --name rate-limiter

# Load the Docker image into KIND
kind load docker-image adaptive-rate-limiter:1.0.0 --name rate-limiter

# Deploy application
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/hpa.yaml

# Deploy monitoring
kubectl apply -f k8s/monitoring/

# Deploy Litmus Chaos (install ChaosCenter first, then)
kubectl apply -f litmus/00-rbac.yaml

# Port-forward to access services
kubectl port-forward -n rate-limiter svc/rate-limiter-service 8080:8080
kubectl port-forward -n monitoring svc/prometheus-service 9090:9090
kubectl port-forward -n monitoring svc/grafana-service 3000:3000
```

---

## API Reference

### Test Endpoints

All endpoints are rate-limited by the interceptor. Responses include rate limit headers.

```bash
# General test endpoint (100 req/60s default)
curl http://localhost:8080/api/test

# Payment endpoint (10 req/60s, FAIL_CLOSED)
curl http://localhost:8080/api/payment

# Search endpoint (200 req/60s, GCRA, FAIL_OPEN)
curl http://localhost:8080/api/search

# AI inference endpoint (50 req/60s, GCRA, FAIL_STRICT)
curl http://localhost:8080/api/ai-inference
```

### Health State

```bash
curl http://localhost:8080/api/health/state
# {"state":"HEALTHY","code":0,"timestamp":1757500000000}
```

### Actuator

```bash
# Prometheus metrics
curl http://localhost:8080/actuator/prometheus

# Health check
curl http://localhost:8080/actuator/health
```

---

## Configuration

Key properties in `application.properties`:

```properties
# Rate limits
rate-limiter.default-limit=100           # Default requests per window
rate-limiter.window-size-seconds=60      # Window duration

# Health probe
rate-limiter.redis-health-check-interval=3000   # Probe interval (ms)

# Hysteresis thresholds
rate-limiter.hysteresis.enter-warning-latency-ms=50.0
rate-limiter.hysteresis.exit-warning-latency-ms=20.0
rate-limiter.hysteresis.warning-stabilization-seconds=15
rate-limiter.hysteresis.degraded-stabilization-seconds=30
rate-limiter.hysteresis.recovery-stabilization-seconds=30

# Quota leasing (disabled by default)
rate-limiter.leasing.enabled=false
rate-limiter.leasing.adaptive=false

# Circuit breaker
resilience4j.circuitbreaker.instances.redis.failure-rate-threshold=50
resilience4j.circuitbreaker.instances.redis.wait-duration-in-open-state=30000ms
```

---

## Testing

```bash
# Run all tests (requires Docker for Testcontainers)
./mvnw test
```

The test suite includes 23 test classes covering:

- Rate limiter integration tests with Testcontainers (real Redis)
- State machine transition logic and hysteresis behavior
- GCRA invariant verification
- Sliding Window correctness
- Cache concurrency tests
- Alert suppression and flap detection
- Quota leasing edge cases
- Benchmark tests

---

## Documentation

Detailed design documents are available in the `docs/` directory:

- **System Architecture** — Full system design and component interactions
- **GCRA vs Sliding Window Decision** — Why two algorithms and when each is used
- **Phase 3 Leasing Design** — Quota leasing architecture and adaptive sizing
- **Architectural Review** — Code quality review and fixes applied
- **Deployment Verification Runbook** — Step-by-step deployment validation
- **User Manual** — End-user guide

---

## License

This project is for educational and demonstration purposes.