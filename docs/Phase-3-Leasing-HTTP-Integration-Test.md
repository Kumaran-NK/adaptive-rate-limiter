# Phase 3 Leasing HTTP Integration Test

## Summary

Implemented the production-readiness integration test for the Phase 3 quota leasing path in `RateLimiterIntegrationTest`.

The test now boots the real Spring Boot application with `SpringBootTest.WebEnvironment.RANDOM_PORT`, sends real network HTTP requests with Java `HttpClient`, connects the production Redis beans to a Redis Testcontainer through `DynamicPropertySource`, and enables leasing only through test properties.

No production defaults were changed.

## Request Path Proven

The exercised path is:

```text
HTTP client
  -> random-port Tomcat server
  -> Spring MVC DispatcherServlet
  -> WebMvcConfig / RateLimitInterceptor
  -> EndpointKeyResolver
  -> RateLimiterService
  -> RateLimiterProperties endpoint strategy lookup
  -> LeaseManager for GCRA endpoints
  -> GcraQuotaAllocator
  -> RedisTemplate
  -> lua/lease_quota.lua in Testcontainers Redis
  -> LocalQuotaBucket
  -> HTTP response headers and status
```

## Files Changed For This Step

- `src/test/java/com/ratelimiter/adaptive_rate_limiter/controller/RateLimiterIntegrationTest.java`
  - Converted the test from `MockMvc` to `SpringBootTest.WebEnvironment.RANDOM_PORT`.
  - Replaced in-process MVC calls with real HTTP requests via Java `HttpClient`.
  - Kept the Redis Testcontainer and `DynamicPropertySource` wiring.
  - Enabled leasing only in test configuration.
  - Tuned `/api/search` to `limit=10`, `strategy=GCRA`, fixed lease sizing, prefetch disabled, and longer lease TTL for deterministic assertions.
  - Added assertions for admitted leased responses, local-bucket admission, Redis lease-call reduction, eventual HTTP 429, and `/api/payment` non-leasing guard.

- `docs/Phase-3-Leasing-HTTP-Integration-Test.md`
  - Documents the implementation, request path, proof strategy, and actual Maven results.

## Existing Worktree State Observed

The final Git status also shows changes that were already part of the workspace state around this task, not production changes introduced for the HTTP test:

```text
M  pom.xml
D  src/main/java/com/ratelimiter/adaptive_rate_limiter/controller/RateLimiterIntegrationTest.java
?? src/test/java/com/ratelimiter/adaptive_rate_limiter/controller/RateLimiterIntegrationTest.java
?? docs/Phase-3-Leasing-HTTP-Integration-Test.md
```

The deleted `src/main/.../RateLimiterIntegrationTest.java` is the old empty production stub location. The implemented test lives under `src/test/java`, which is the correct location for the real integration test.

## What The Test Proves

### Search Uses Leased GCRA Over Real HTTP

`/api/search` is configured as a GCRA endpoint in the test context. The test sends real HTTP requests to the random-port server and asserts every admitted response includes:

```text
X-Algorithm-Used: GCRA_LEASED
```

This proves the request passed through the interceptor and `RateLimiterService` into the leased GCRA path.

### Leasing Was Genuinely Active

The test snapshots the real singleton `LeaseManager` counters before and after the `/api/search` requests.

It asserts:

```text
syncLeaseCount delta > 0
localAdmitCount delta > 0
prefetchLeaseCount delta == 0
```

That means Redis quota was acquired by `LeaseManager`, then later admitted requests were served from `LocalQuotaBucket` without a Redis call. Prefetch is disabled, so the local-admit evidence is deterministic and not dependent on async timing.

### Redis-Call Reduction Was Demonstrated

The test asserts this invariant rather than a brittle exact count:

```text
admitted HTTP requests > syncLeaseCount delta
admitted HTTP requests > syncLeaseCount delta + prefetchLeaseCount delta
```

With prefetch disabled, this proves multiple admitted HTTP requests were served locally after Redis granted quota, instead of contacting Redis once per admitted request.

### Eventual 429 Was Verified

The test configures `/api/search` to a small deterministic quota:

```text
rate-limiter.endpoints.search.limit=10
rate-limiter.leasing.max-lease-fraction=0.5
rate-limiter.leasing.prefetch-watermark=0.0
```

It sends 10 admitted requests with the same `X-API-Key`, then sends one more immediate request and asserts:

```text
HTTP 429
X-Algorithm-Used: GCRA_LEASED
```

This verifies the leased path still denies correctly when quota is exhausted.

### Payment Remains Non-Leased

The `/api/payment` test uses a different key, sends real HTTP requests, and asserts:

```text
HTTP 200
X-Algorithm-Used does not contain LEASED
LeaseManager counters unchanged
```

This verifies the EXACT_QUOTA guard remains intact even while leasing is globally enabled in the test context.

## Implementation Note

An initial attempt used `TestRestTemplate`, but this Spring Boot 4 test runtime did not provide it on the classpath and produced `NoClassDefFoundError: TestRestTemplate`. The final test uses Java's built-in `HttpClient`, which still sends real HTTP to the random-port server and avoids adding new test dependencies.

## Test Results

### Focused HTTP Integration Test

Command:

```text
mvn test -Dtest=RateLimiterIntegrationTest
```

Result:

```text
Tests run: 2
Failures: 0
Errors: 0
Skipped: 0
Build result: BUILD SUCCESS
Total time: 25.900 s
Finished: 2026-08-28T10:27:17+05:30
```

### Relevant Leasing Tests

Command:

```text
mvn test "-Dtest=LeaseManagerTest,GcraQuotaAllocatorTest,RateLimiterServiceLeasingGuardTest"
```

Result:

```text
Tests run: 16
Failures: 0
Errors: 0
Skipped: 0
Build result: BUILD SUCCESS
Total time: 16.910 s
Finished: 2026-08-28T10:28:32+05:30
```

### Full Maven Test Suite

Command:

```text
mvn test
```

Result:

```text
Tests run: 75
Failures: 0
Errors: 0
Skipped: 1
Build result: BUILD SUCCESS
Total time: 01:10 min
Finished: 2026-08-28T10:29:49+05:30
```

## Genuine Bugs Discovered

No production wiring or architecture bugs were discovered.

The only issue encountered was with the attempted test client choice: `TestRestTemplate` was not available in this Spring Boot 4 test runtime. The implementation was adjusted to use JDK `HttpClient` without changing production code or adding a new dependency.
