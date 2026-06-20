"""
test_collector.py

Standalone script to verify the Collector module against your live AKS
cluster. Run this from inside the cluster (as a pod) or via port-forward
from your laptop.

To run from your laptop against the live cluster, first port-forward
Prometheus in a separate terminal:

    kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 9090:9090

Then run this script:

    python test_collector.py

Expected output: one MetricSnapshot printed to the console, showing
CPU utilisation and replica count for every service in alomp_config.yaml,
plus request rate / p99 latency / error rate for the frontend (will show
None for the latter three until Locust is deployed and actively driving
traffic -- this is expected and not an error).
"""

import logging
import sys

from acomp.collector import Collector
from acomp.context_map import load_context_map

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("test_collector")


def main():
    context_map = load_context_map("alomp_config.yaml")
    service_names = context_map.service_names()
    logger.info("Context Map loaded. Services: %s", service_names)

    collector = Collector(
        prometheus_url="http://localhost:9090",  # via kubectl port-forward
        namespace="default",
        service_names=service_names,
        entry_point_service="frontend",
    )

    logger.info("Polling Prometheus...")
    snapshot = collector.poll()

    print(f"\n=== MetricSnapshot @ {snapshot.timestamp.isoformat()} ===\n")
    print(f"{'Service':<28} {'CPU%':>8} {'Replicas':>9} {'Req/s':>8} {'p99 ms':>8} {'Err%':>7}")
    print("-" * 76)
    for name in sorted(snapshot.services.keys()):
        m = snapshot.services[name]
        cpu = f"{m.cpu_utilisation*100:.1f}" if m.cpu_utilisation is not None else "--"
        replicas = str(m.replica_count) if m.replica_count is not None else "--"
        rps = f"{m.request_rate:.2f}" if m.request_rate is not None else "--"
        p99 = f"{m.latency_p99_ms:.0f}" if m.latency_p99_ms is not None else "--"
        err = f"{m.error_rate*100:.1f}" if m.error_rate is not None else "--"
        print(f"{name:<28} {cpu:>8} {replicas:>9} {rps:>8} {p99:>8} {err:>7}")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
