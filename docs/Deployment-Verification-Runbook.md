# Deployment and Verification Runbook

This document records the deployment work completed for the Adaptive Self-Healing Rate Limiter and the commands used to verify it.

## Prerequisites

- Windows PowerShell
- Java 21
- Docker Desktop with Docker Engine running
- kubectl
- kind

Minikube was not installed on the workstation, so Kubernetes verification used kind. The manifests are also suitable for Minikube.

## Build and Test

Run from the project directory:

```powershell
cd 'D:\RATE LIMITER V2\adaptive-rate-limiter'
.\mvnw.cmd clean package
```

Result: build succeeded, 83 tests ran, 0 failed, and 1 test was skipped.

Focused regression test:

```powershell
.\mvnw.cmd -Dtest=RateLimiterServiceStrategySelectionTest test
```

## Docker Image

```powershell
docker version --format '{{.Server.Version}}'
docker build -t adaptive-rate-limiter:1.0.0 .
docker images adaptive-rate-limiter
```

## Compose Smoke Test

The Compose file runs Redis and the application on one Docker network. The application receives REDIS_HOST=redis.

```powershell
docker compose config
docker compose up -d redis app
docker compose ps
```

Health checks:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/health
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/health/liveness
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/health/readiness
```

Expected result: HTTP 200 and an UP health response.

## Exact-Quota Test

The payment endpoint is configured as a 10-request Sliding Window quota.

```powershell
$key = 'compose-exact-' + [guid]::NewGuid()
for ($i = 1; $i -le 11; $i++) {
    try {
        $r = Invoke-WebRequest -UseBasicParsing `
            -Headers @{ 'X-API-Key' = $key } `
            http://localhost:8080/api/payment
        "request $i status=$($r.StatusCode) algorithm=$($r.Headers['X-Algorithm-Used']) remaining=$($r.Headers['X-RateLimit-Remaining'])"
    } catch {
        $response = $_.Exception.Response
        "request $i status=$([int]$response.StatusCode)"
    }
}
```

Expected result: requests 1-10 return 200, request 11 returns 429, and the algorithm is SLIDING_WINDOW.

## Leasing Test

Leasing is disabled by default. Start a separate experiment container with leasing enabled:

```powershell
docker run -d `
  --name adaptive-rate-limiter-leased `
  --network adaptive-rate-limiter_default `
  -p 8081:8080 `
  -e REDIS_HOST=redis `
  -e REDIS_PORT=6379 `
  -e POD_COUNT=1 `
  -e RATE_LIMITER_LEASING_ENABLED=true `
  -e RATE_LIMITER_LEASING_ADAPTIVE=false `
  -e RATE_LIMITER_LEASING_PREFETCH_WATERMARK=0.0 `
  -e RATE_LIMITER_LEASING_MAX_LEASE_FRACTION=0.25 `
  -e RATE_LIMITER_LEASING_MIN_LEASE=1 `
  -e RATE_LIMITER_LEASING_LEASE_TTL_MS=30000 `
  -e RATE_LIMITER_REDIS_HEALTH_CHECK_INITIAL_DELAY=60000 `
  -e RATE_LIMITER_HYSTERESIS_ENTER_WARNING_LATENCY_MS=1000 `
  -e RATE_LIMITER_ENDPOINTS_SEARCH_LIMIT=20 `
  -e RATE_LIMITER_ENDPOINTS_SEARCH_STRATEGY=GCRA `
  -e RATE_LIMITER_ENDPOINTS_SEARCH_DEGRADATION_MODE=FAIL_OPEN `
  adaptive-rate-limiter:1.0.0
```

Generate traffic and inspect metrics:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8081/api/health/state
$key = 'leased-search-' + [guid]::NewGuid()
for ($i = 1; $i -le 21; $i++) {
    Invoke-WebRequest -UseBasicParsing `
      -Headers @{ 'X-API-Key' = $key } `
      http://localhost:8081/api/search
}
$metrics = (Invoke-WebRequest -UseBasicParsing http://localhost:8081/actuator/prometheus).Content
$metrics -split "`n" | Where-Object { $_ -match '^rate_limiter_lease_' }
```

Observed result: 20 requests were admitted through four leases of five, saving 16 Redis calls; request 21 returned 429.

Cleanup:

```powershell
docker rm -f adaptive-rate-limiter-leased
```

## Kubernetes Cluster

Create the local kind cluster:

```powershell
kind create cluster --name rate-limiter --wait 120s
kubectl config current-context
```

Load the locally built images:

```powershell
kind load docker-image adaptive-rate-limiter:1.0.0 --name rate-limiter
kind load docker-image redis:7-alpine --name rate-limiter
```

Deploy the baseline without HPA:

```powershell
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/deployment.yaml
kubectl rollout status deployment/redis -n rate-limiter --timeout=120s
kubectl rollout status deployment/adaptive-rate-limiter -n rate-limiter --timeout=240s
kubectl get pods -n rate-limiter -o wide
kubectl get svc -n rate-limiter
```

The Deployment uses two application replicas, POD_COUNT=2, Redis Service redis-service, and a startup probe for Spring Boot startup time.

## Kubernetes Service Test

```powershell
kubectl port-forward svc/rate-limiter-service 18080:8080 -n rate-limiter
```

Repeat the exact-quota test against http://localhost:18080/api/payment. Expected result: 10 successful requests and a 429 on request 11.

Important: kubectl port-forward service selects a backend pod. It is a Service smoke test, not proof of load balancing across both replicas.

## Cross-Pod Shared-Quota Test

```powershell
kubectl get pods -n rate-limiter -l app=rate-limiter -o jsonpath='{.items[*].metadata.name}'
kubectl port-forward pod/<pod-1> 18081:8080 -n rate-limiter
kubectl port-forward pod/<pod-2> 18082:8080 -n rate-limiter
```

Send six requests to port 18081 and four to port 18082 using one API key. Send request 11 to either port. The observed result was 10 successful requests followed by 429, proving both pods coordinate through Redis.

## Redis Failure and Recovery

```powershell
docker stop adaptive-rate-limiter-redis-1
Start-Sleep -Seconds 5
Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/health/state
docker start adaptive-rate-limiter-redis-1
```

During failure, search used TOKEN_BUCKET/DEGRADED. After Redis was restored, the state machine progressed through DEGRADED -> RECOVERY -> HEALTHY.

## HPA and Chaos Status

```powershell
kubectl apply --dry-run=server -f k8s/hpa.yaml
kubectl get --raw /apis/metrics.k8s.io/v1beta1
```

The HPA manifest is valid, but metrics-server is not installed. LitmusChaos is also not installed, so formal Litmus experiments remain a follow-up.

## Final Status

```powershell
kubectl get pods -n rate-limiter
kubectl get deployments -n rate-limiter
docker compose ps
git diff --check
```

