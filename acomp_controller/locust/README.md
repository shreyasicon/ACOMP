# ACOMP Load Generator

Custom Locust load generator for the ACOMP evaluation, targeting Online
Boutique's frontend service. Unlike Online Boutique's own bundled
`loadgenerator`, this version exports Prometheus-native metrics on port 9646
so the ACOMP Collector can read request rate, p99 latency, and error rate
without a separate exporter sidecar or undocumented third-party metric names.

## Why a custom load generator instead of Online Boutique's built-in one

Online Boutique ships its own `loadgenerator` Deployment (you saw it running
already in `kubectl get pods`). It works fine for generating traffic, but it
does not expose a Prometheus `/metrics` endpoint -- it only prints stats to
its own logs. Since ACOMP's Collector needs request rate, p99 latency, and
error rate as numeric time series it can query via PromQL, this custom
version wires Locust's own request-tracking events directly into
`prometheus_client` counters/histograms, giving full control over exact
metric names and avoiding dependency on an unmaintained third-party Go
exporter binary.

## Files

| File | Purpose |
|---|---|
| `locustfile.py` | Shopper behaviour (browse/cart/checkout) + Prometheus metric export |
| `requirements.txt` | `locust` + `prometheus_client` |
| `Dockerfile` | Builds the image |
| `build_and_push.sh` | Builds for linux/amd64 and pushes to acompregistry |
| `k8s-manifests.yaml` | Deployment + Service + ServiceMonitor |

## Deployment steps

1. **Build and push the image** (from Azure Cloud Shell, where Docker and
   `az` are both available):

   ```bash
   cd locust
   bash build_and_push.sh
   ```

   This builds explicitly for `linux/amd64` -- required, since your cluster
   hit the ARM/x86 `exec format error` issue earlier with the wrong
   architecture image.

2. **Apply the Kubernetes manifests:**

   ```bash
   kubectl apply -f k8s-manifests.yaml
   ```

3. **Verify the pod is running and on the x86 pool:**

   ```bash
   kubectl get pods -l app=acomp-loadgenerator -o wide
   ```

4. **Verify Prometheus is scraping it.** Port-forward Prometheus and check
   the Targets page:

   ```bash
   kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 9090:9090
   ```

   Then open `http://localhost:9090/targets` in a browser and confirm a
   target named `acomp-loadgenerator` (or similar, derived from the
   ServiceMonitor) shows as `UP`.

5. **Confirm metrics are flowing** by querying directly:

   ```bash
   curl http://localhost:9090/api/v1/query?query=acomp_locust_requests_total
   ```

   You should see non-empty results once the load generator has been
   running for at least a few seconds.

## Adjusting load

The Deployment args control load shape:

```yaml
args:
  - "--host=http://frontend"
  - "--headless"
  - "--users=10"        # concurrent simulated users
  - "--spawn-rate=2"     # users started per second during ramp-up
  - "--run-time=1h"      # auto-stop after this duration
```

For the thesis evaluation scenarios (Scenario 1: Steady to Bursty Load,
Scenario 2: Sustained High-Pressure Load, Scenario 3: Downstream Degradation
Injection), these values should be edited per scenario and the Deployment
re-applied, or replaced with Locust's `--users`/`--spawn-rate` controlled
externally via the Locust web UI / REST API for finer control over ramp
shape. A dedicated scenario-runner script is a sensible next build step
once the Policy Engine is in place and full end-to-end runs are needed.

## Stopping the load generator

```bash
kubectl scale deployment acomp-loadgenerator --replicas=0
```

Or delete entirely:

```bash
kubectl delete -f k8s-manifests.yaml
```
