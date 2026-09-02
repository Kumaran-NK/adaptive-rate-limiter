# Adaptive Rate Limiter User Manual

## What It Does

The service protects API endpoints with a Redis-backed distributed rate limiter. It selects the algorithm and failure policy per endpoint.

| Endpoint | Default limit | Algorithm | Failure policy |
|---|---:|---|---|
| /api/payment | 10 per 60 seconds | Sliding Window | FAIL_CLOSED |
| /api/sms | 5 per 60 seconds | Sliding Window | FAIL_CLOSED |
| /api/ai-inference | 50 per 60 seconds | GCRA | FAIL_STRICT |
| /api/search | 200 per 60 seconds | GCRA | FAIL_OPEN |

The endpoint name is resolved from the URL. For example, /api/payment maps to the payment policy.

## Starting Locally

### Docker Compose

```powershell
docker compose up -d redis app
docker compose ps
```

The application is available at http://localhost:8080.

Stop the local services:

```powershell
docker compose down
```

### Running the JAR Directly

Start Redis first, then run:

```powershell
docker run -d --name local-redis -p 6379:6379 redis:7-alpine
.\mvnw.cmd spring-boot:run
```

Set REDIS_HOST and REDIS_PORT when Redis is not on localhost:6379.

## Calling the API

Supply an API key with X-API-Key. If no API key is supplied, the service falls back to the client remote address.

```powershell
Invoke-WebRequest -UseBasicParsing `
  -Headers @{ 'X-API-Key' = 'user123' } `
  http://localhost:8080/api/search
```

Responses include:

- X-RateLimit-Remaining: remaining quota
- X-RateLimit-Reset: reset or retry timestamp in epoch milliseconds
- X-System-Health: HEALTHY, WARNING, DEGRADED, or RECOVERY
- X-Algorithm-Used: SLIDING_WINDOW, GCRA, or GCRA_LEASED

HTTP 429 means the quota was exceeded. HTTP 503 means a safe decision could not be produced by the configured path.

## Health and Metrics

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/health
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/health/liveness
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/health/readiness
Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/health/state
Invoke-WebRequest -UseBasicParsing http://localhost:8080/actuator/prometheus
```

Useful metrics include:

- rate_limiter_requests_total
- rate_limiter_health_state
- rate_limiter_state_transitions_total
- rate_limiter_lease_grants_total
- rate_limiter_lease_redis_calls_saved_total
- rate_limiter_lease_wasted_quota_total
- rate_limiter_lease_starvation_total

## Health States

### HEALTHY

Normal Redis-backed operation. Exact-quota endpoints use direct Sliding Window checks. GCRA endpoints may use direct GCRA or leasing when leasing is enabled.

### WARNING

Redis is reachable but latency or error rate is elevated. GCRA/rate-pacing endpoints may use the local cache to reduce Redis traffic. Exact-quota Sliding Window endpoints continue using the distributed path so their hard ceiling is preserved.

### DEGRADED

Redis is unavailable or the circuit breaker is open.

- FAIL_CLOSED: deny requests.
- FAIL_STRICT: use a local Token Bucket divided by the configured pod count.
- FAIL_OPEN: use a local Token Bucket at the full endpoint limit.

### RECOVERY

Redis has returned, but the service gradually resumes normal distributed behavior after stabilization.

## Enabling Leasing

Leasing is off by default. It is only eligible for GCRA/rate-pacing endpoints. Exact-quota Sliding Window endpoints never use leasing.

```properties
rate-limiter.leasing.enabled=true
rate-limiter.leasing.adaptive=false
rate-limiter.leasing.min-lease=1
rate-limiter.leasing.max-lease-fraction=0.25
rate-limiter.leasing.prefetch-watermark=0.2
rate-limiter.leasing.lease-ttl-ms=5000
```

Leasing reduces Redis calls by admitting several requests from a local quota lease. It trades exact rolling-window semantics for fewer Redis calls, so use it for rate/pacing endpoints rather than payments.

## Kubernetes Operations

Select the kind context:

```powershell
kubectl config use-context kind-rate-limiter
```

Deploy or update the baseline:

```powershell
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/deployment.yaml
```

Inspect status and logs:

```powershell
kubectl get pods -n rate-limiter
kubectl logs deployment/adaptive-rate-limiter -n rate-limiter
kubectl describe pods -n rate-limiter
```

Access the Service:

```powershell
kubectl port-forward svc/rate-limiter-service 18080:8080 -n rate-limiter
```

The current development baseline uses two application pods and one Redis pod. It is not Redis high availability.

## Safe Shutdown and Cleanup

Compose:

```powershell
docker compose down
```

Kubernetes namespace:

```powershell
kubectl delete namespace rate-limiter
```

Kind cluster:

```powershell
kind delete cluster --name rate-limiter
```

Only run deletion commands when you intend to remove the local deployment and its state.

## Known Limitations

- HPA is defined but should remain unapplied until metrics-server is installed.
- POD_COUNT=2 is static for the baseline; dynamic pod discovery is required before HPA scaling.
- LitmusChaos is not installed yet.
- The default Kubernetes Redis deployment has one pod and no persistence or failover.
- Prometheus scraping is configured, but Grafana dashboards still need to be created and deployed.

