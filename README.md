# Adaptive Self-Healing Rate Limiter

A distributed rate limiter that automatically detects system degradation and heals itself without human intervention.

## System States

| State | Algorithm | Data Source | Consistency |
|-------|-----------|-------------|-------------|
| HEALTHY | Sliding Window | Redis | 100% |
| WARNING | Sliding Window (cached) | Redis + Local | ~95% |
| DEGRADED | Token Bucket | Local only | ~85% |
| RECOVERY | Sliding Window | Redis (gradual) | Ramping to 100% |

## Tech Stack

- **Framework:** Spring Boot 4.1.0 (Java 21)
- **Distributed Store:** Redis with Lua scripting
- **Local Fallback:** Caffeine cache
- **Resilience:** Resilience4j Circuit Breaker
- **Observability:** Micrometer → Prometheus → Grafana
- **Orchestration:** Kubernetes with HPA auto-scaling
- **Chaos Testing:** LitmusChaos

## Quick Start

```bash
# Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# Build and run
./mvnw spring-boot:run

# Test
curl http://localhost:8080/api/test