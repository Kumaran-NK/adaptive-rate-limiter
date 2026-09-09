# LitmusChaos Experiment Manifests

This directory contains all chaos experiment YAML files for testing the
**adaptive-rate-limiter** system's resilience.

## Prerequisites

1. **LitmusChaos 3.0** must be installed in your cluster.
2. The `pod-delete`, `pod-cpu-hog`, `pod-network-loss`, `pod-network-latency`,
   and `pod-memory-hog` experiment CRDs must be installed in the `rate-limiter`
   namespace:

   ```bash
   LITMUS_VERSION=3.0.0
   NS=rate-limiter

   for exp in pod-delete pod-cpu-hog pod-network-loss pod-network-latency pod-memory-hog; do
     kubectl apply -f "https://hub.litmuschaos.io/api/chaos/${LITMUS_VERSION}?file=charts/generic/${exp}/experiment.yaml" -n $NS
   done
   ```

3. Apply the shared RBAC (only once):

   ```bash
   kubectl apply -f 00-rbac.yaml
   ```

## Test Matrix

| #  | File                            | Experiment            | Target          | What We Verify                                           |
|----|-------------------------------- |-----------------------|-----------------|----------------------------------------------------------|
| 1  | `01-pod-delete.yaml`            | pod-delete (graceful) | rate-limiter    | K8s recreates the pod, service continues                 |
| 2  | `02-pod-cpu-hog.yaml`           | pod-cpu-hog           | rate-limiter    | Responsiveness under CPU pressure                        |
| 3  | `03-pod-network-loss.yaml`      | pod-network-loss      | rate-limiter    | Degradation mode activates on network failure            |
| 4  | `04-pod-network-latency.yaml`   | pod-network-latency   | rate-limiter    | Graceful handling of slow Redis responses                |
| 5  | `05-redis-failure.yaml`         | pod-delete            | redis           | Fallback policy activates on Redis outage                |
| 6  | `06-pod-memory-hog.yaml`        | pod-memory-hog        | rate-limiter    | Behavior/recovery under memory pressure                  |
| 7  | `07-pod-kill.yaml`              | pod-delete (forced)   | rate-limiter    | Recovery after ungraceful termination (SIGKILL)          |
| 8  | `08-combined-failure.yaml`      | workflow (multi-fault) | both           | Overall resilience under simultaneous failures           |

## Running a Test

### Via Litmus 3.0 ChaosCenter UI

1. Navigate to **Chaos Experiments → New Experiment**.
2. Select **Upload YAML**.
3. Upload the desired YAML file.

### Via kubectl

```bash
# Run a single experiment (e.g., Test 1)
kubectl apply -f 01-pod-delete.yaml

# Check the ChaosEngine status
kubectl get chaosengine -n rate-limiter

# Check the ChaosResult
kubectl get chaosresult -n rate-limiter

# Describe a specific result for details
kubectl describe chaosresult rate-limiter-pod-delete-pod-delete -n rate-limiter
```

### Cleanup

```bash
# Delete a specific experiment
kubectl delete chaosengine <engine-name> -n rate-limiter

# Delete all chaos engines
kubectl delete chaosengine --all -n rate-limiter
```

## Key Design Decisions

- **Test 5 (Redis Failure)** targets `app=redis` instead of `app=rate-limiter` —
  this is intentional to simulate the infrastructure dependency failing.
- **Test 7 vs Test 1**: Both use `pod-delete`, but Test 7 sets `FORCE: "true"`
  (SIGKILL) while Test 1 uses graceful deletion.
- **Test 8 (Combined Failure)** uses an Argo Workflow to run two faults in
  parallel, testing the system under simultaneous app + infrastructure failures.
- **Memory Hog (Test 6)** consumes 400Mi against a 512Mi limit — this creates
  heavy pressure without triggering an instant OOMKill.
