# Architectural Review Fix Report: Concurrency & Correctness Improvements

This document details the root cause analysis, architectural risk, and exact resolutions for the 6 concurrency, clock-skew, and connection lifecycle issues identified during the architectural review.

---

## Summary of Fixes

| # | Priority | Component | Issue Description | Fix Summary |
|---|---|---|---|---|
| **1** | 🔴 P0 | `RedisHealthProbe` | Redis connection leak per health check | Replaced raw `getConnection().ping()` with Spring-managed `redisTemplate.execute(RedisCallback)` |
| **2** | 🔴 P0 | `SlidingWindowStrategy` | Sorted Set uses application node clock | Migrated `sliding_window.lua` to Redis server clock (`redis.call('TIME')`) |
| **3** | 🟠 P1 | `StateMachine` | Non-atomic `currentState` and `stateEnteredAt` | Unified state & timestamp updates under `synchronized` methods |
| **4** | 🟠 P1 | `StateMachine` | TOCTOU race condition in state transitions | Atomically guarded `evaluateHealth()` with `synchronized` |
| **5** | 🟡 P2 | `AlertSuppressionService` | Unsafe shared state mutation in transition history | Synchronized `onStateTransition()` protecting all private state operations |
| **6** | 🟡 P2 | `RedisHealthProbe` | Latency and error window calculation desynchronization | Added coarse-grained `windowLock` around atomic post-PING observation cycle |

---

## Detailed Root Cause Analysis & Resolutions

### 🔴 Fix 1: `RedisHealthProbe` Connection Leak (P0)

#### Root Cause
In `RedisHealthProbe.java`, raw connections were acquired directly from the connection factory:
```java
// BEFORE (Leaked connections)
String result = redisTemplate.getConnectionFactory()
        .getConnection().ping();
```
`getConnection()` borrows a connection from the underlying Jedis/Lettuce pool, but because `.close()` was never called, connections accumulated on every probe execution (every 3 seconds). Over time, this pool exhaustion caused Redis operations to fail, triggering false-positive circuit breaker trips.

#### Resolution
Replaced manual connection access with Spring's `RedisCallback`, allowing Spring Data Redis to manage the connection borrowing and releasing lifecycle automatically:
```java
// AFTER (Managed lifecycle)
String result = redisTemplate.execute(
        (RedisCallback<String>) connection -> connection.ping()
);
```

---

### 🔴 Fix 2: Sliding Window Application Clock Skew (P0)

#### Root Cause
`SlidingWindowStrategy.java` previously generated client-side timestamps using `System.currentTimeMillis()` and passed them into `sliding_window.lua`. In multi-pod Kubernetes clusters, clock drift between application nodes caused inconsistent trimming and admission decisions in the shared Redis sorted set.

#### Resolution
1. Updated `sliding_window.lua` to derive the current time directly from the Redis server clock via `redis.call('TIME')`:
```lua
local time_parts = redis.call('TIME')
local now = math.floor(tonumber(time_parts[1]) * 1000 + tonumber(time_parts[2]) / 1000)
```
2. Updated `SlidingWindowStrategy.java` to remove the application timestamp parameter from script execution arguments.

---

### 🟠 Fixes 3 & 4: `StateMachine` Compound State & TOCTOU Race Conditions (P1)

#### Root Cause
`StateMachine.java` suffered from two distinct concurrency issues:
1. **Compound State Atomicity**: `currentState` was wrapped in an `AtomicReference`, but `stateEnteredAt` was a plain `Instant`. Readers could observe a newly updated state combined with a stale timestamp.
2. **TOCTOU Race Condition**: Concurrent callers to `evaluateHealth()` could both evaluate transitions from the same initial state, resulting in invalid interleaved transitions.

#### Resolution
Since `evaluateHealth()` runs periodically on a single health probe schedule (~every 3 seconds), an intrinsic lock (`synchronized`) provided clean, contention-free atomicity across state reads, evaluation logic, and timestamp updates.

1. Replaced `AtomicReference<HealthState>` with a plain `HealthState currentState` field.
2. Marked `evaluateHealth()` and `getCurrentState()` as `synchronized`:
```java
public synchronized HealthState getCurrentState() {
    return currentState;
}

public synchronized StateTransition evaluateHealth(HealthCheckEvent event) {
    // Atomic read, state evaluation, state transition, and timestamp update
    ...
}
```

---

### 🟡 Fix 5: `AlertSuppressionService` Thread Safety (P2)

#### Root Cause
`AlertSuppressionService` stored state transition history in a plain `ArrayList` (`recentTransitions`) and mutated tracking variables (`warningStartedAt`, `warningAlertSent`, `degradedAlertSent`).

#### Audit & Resolution
An audit confirmed that all 7 helper methods mutating or querying these fields (`handleWarningState`, `handleDegradedState`, `handleRecoveryState`, `handleHealthyState`, `countRecentFlaps`, `sustainedFor`, `cleanupOldTransitions`) are **private** and accessible only via the main entry point `onStateTransition()`.

Marking `onStateTransition()` as `synchronized` encapsulates the entire state boundary and makes compound history trimming and alert decision-making atomic:
```java
public synchronized void onStateTransition(StateTransition transition) {
    if (transition == null) return;
    ...
}
```

---

### 🟡 Fix 6: `RedisHealthProbe` Observation Cycle Atomicity (P2)

#### Root Cause
`latencyWindow` and `errorWindow` were updated and calculated using independent calls. Interleaved execution could allow percentiles and error rates to be calculated from mismatched window states.

#### Resolution
Introduced an explicit `windowLock` object. In `RedisHealthProbe.checkHealth()`, the network PING operates **outside** the lock, while window updates and metric calculations are executed as a single atomic snapshot **inside** the lock:
```java
// Redis I/O occurs outside lock
String result = redisTemplate.execute((RedisCallback<String>) connection -> connection.ping());
latencyMs = System.currentTimeMillis() - start;

// Atomic observation cycle
synchronized (windowLock) {
    addToLatencyWindow(latencyMs);
    addToErrorWindow(!reachable);

    p50 = calculatePercentile(50);
    p95 = calculatePercentile(95);
    p99 = calculatePercentile(99);
    errorRate = calculateErrorRate();
}
```

---

## Verification & Status

- ✅ All changes compiled and verified with unit test suite.
- ✅ Fixes committed to repository codebase.
