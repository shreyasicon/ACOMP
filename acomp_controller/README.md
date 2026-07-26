# ACOMP — Adaptive and Contextually Orchestrative Microservice Pipeline

MSc Cloud Computing Research Project  
Sai Shreyas Gubbi Harish | x24194956  
National College of Ireland  
Supervisor: Dr. Aqeel Kazmi

---

## What is ACOMP?

ACOMP is a deterministic, explainable Kubernetes orchestration layer for distributed microservice pipelines. Unlike native Kubernetes HPA which scales each service in isolation, ACOMP treats the entire pipeline as the primary unit of scaling governance.

Every 15 seconds, ACOMP:
1. Collects CPU, latency, error rate and request rate from all pipeline services via Prometheus
2. Classifies the pipeline into one of four states: **HEALTHY**, **UPSTREAM_LOAD_PRESSURE**, **DOWNSTREAM_DEGRADATION**, or **PIPELINE_CEILING**
3. Computes scaling decisions using empirically calibrated work factors to propagate pressure across dependent services
4. Applies decisions to the Kubernetes API in upstream-first dependency order
5. Writes a structured JSON Lines audit record per cycle — enabling full operational accountability without machine learning expertise

---

## Repository Structure

```
ACOMP/
├── acomp_controller/          # ACOMP controller — the research contribution
│   ├── acomp/                 # Core Python modules
│   │   ├── __init__.py
│   │   ├── context_map.py     # Loads alomp_config.yaml dependency graph
│   │   ├── collector.py       # Polls Prometheus every 15s
│   │   ├── policy_engine.py   # Algorithm 1: state classification + scaling
│   │   ├── actuator.py        # Applies decisions to Kubernetes API
│   │   └── decision_logger.py # Writes JSON Lines audit records
│   ├── locust/                # Custom load generator with Prometheus export
│   │   ├── locustfile.py
│   │   ├── Dockerfile
│   │   └── k8s-manifests.yaml
│   ├── scripts/               # Evaluation and calibration scripts
│   │   ├── calibrate_work_factors.py   # Empirical work factor calibration
│   │   ├── run_scenario.py             # Automated scenario runner (8 scenarios)
│   │   ├── analyse_results.py          # Comparison table generator
│   │   └── run_full_evaluation.sh      # Full evaluation pipeline
│   ├── main.py                # Controller entry point (15s control loop)
│   ├── Dockerfile             # Controller container image
│   ├── k8s-manifests.yaml     # Kubernetes deployment (RBAC + ConfigMap + Deployment)
│   ├── alomp_config.yaml      # Context Map: dependency graph + work factors
│   ├── requirements.txt
│   ├── test_collector_offline.py
│   ├── test_policy_engine.py
│   ├── test_actuator_offline.py
│   └── test_decision_logger_offline.py
└── microservices-demo/        # Google Online Boutique (test application, Apache 2.0)
```

---

## Prerequisites

- Azure AKS cluster (3 × Standard_D2s_v3 nodes, x86)
- Azure Container Registry (`acompregistry`)
- `kubectl` connected to the cluster
- `helm` installed
- Python 3.11+

---

## Deployment

### 1. Deploy monitoring stack
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
kubectl create namespace monitoring
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set grafana.enabled=true \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

### 2. Deploy Online Boutique
```bash
cd microservices-demo
kubectl apply -f ./release/kubernetes-manifests.yaml
for d in adservice cartservice checkoutservice currencyservice emailservice frontend \
  loadgenerator paymentservice productcatalogservice recommendationservice \
  redis-cart shippingservice; do
  kubectl patch deployment $d -p '{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/arch":"amd64"}}}}}'
done
```

### 3. Set CPU requests on key services
```bash
for svc in frontend currencyservice productcatalogservice cartservice recommendationservice checkoutservice; do
  kubectl set resources deployment $svc --requests=cpu=100m --limits=cpu=500m
done
```

### 4. Build and deploy the Locust load generator
```bash
cd acomp_controller/locust
az acr build --registry acompregistry --image acomp-loadgenerator:v1 --platform linux/amd64 .
kubectl apply -f k8s-manifests.yaml
```

### 5. Build and deploy the ACOMP controller
```bash
cd acomp_controller
az acr build --registry acompregistry --image acomp-controller:v1 --platform linux/amd64 .
kubectl apply -f k8s-manifests.yaml
```

### 6. Verify everything is running
```bash
kubectl get pods
kubectl logs -l app=acomp-controller --tail=10
```

---

## Calibration

Before running evaluation scenarios, calibrate the work factors in `alomp_config.yaml`:

```bash
cd acomp_controller
kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 9090:9090 &
source venv/bin/activate
python3 scripts/calibrate_work_factors.py
kubectl rollout restart deployment/acomp-controller
```

---

## Running Evaluation Scenarios

```bash
cd acomp_controller

# Run a single scenario
python3 scripts/run_scenario.py --scenario 1 --comparator acomp
python3 scripts/run_scenario.py --scenario 1 --comparator baseline_a

# Run full evaluation (all 3 thesis scenarios, ACOMP + Baseline A)
bash scripts/run_full_evaluation.sh

# Generate comparison tables from results
python3 scripts/analyse_results.py
```

### Scenario Reference

| # | Name | What it tests |
|---|------|--------------|
| 1 | Steady to Bursty Load | UPSTREAM_LOAD_PRESSURE detection and propagation |
| 2 | Sustained High-Pressure | Oscillation index under sustained load |
| 3 | Downstream Degradation | DOWNSTREAM_DEGRADATION classification and scaling suppression |
| 4 | Pipeline Ceiling | Guardrail enforcement at max_replicas |
| 5 | Unable to Scale | RBAC error handling and audit completeness under failure |
| 6 | Prometheus Unavailable | Collector resilience and graceful cycle skipping |
| 7 | Rapid Load Oscillation | Anti-thrash idempotency behaviour |
| 8 | Controller Cold Start | Stateless recovery after mid-load pod restart |

---

## Running Tests

```bash
cd acomp_controller
source venv/bin/activate
python3 test_collector_offline.py
python3 test_policy_engine.py
python3 test_actuator_offline.py
python3 test_decision_logger_offline.py
```

---

## Cost Management

Always stop the cluster after each session:
```bash
az aks stop --resource-group acomp-rg --name acomp-cluster
```

---

## Architecture

```
External Traffic (Locust)
        ↓
Google Online Boutique (11 services on AKS)
        ↓ metrics every 15s
Prometheus
        ↓
ACOMP Controller Pod
  ├── Collector      → reads Prometheus
  ├── Policy Engine  → classifies state, computes decisions
  ├── Actuator       → patches Kubernetes Deployments
  └── Decision Logger → writes JSON Lines audit records
        ↓                        ↓
Kubernetes API          Azure Monitor Logs
(scaling actions)       (decision audit trail)
```

---

## Attribution

`microservices-demo/` contains Google's Online Boutique, licensed under Apache 2.0.  
All code in `acomp_controller/` is original work by Sai Shreyas Gubbi Harish (x24194956).
