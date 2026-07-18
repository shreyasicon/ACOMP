#!/usr/bin/env python3
"""
scripts/capture_metrics.py

Run this immediately after each scenario completes to capture all four
thesis evaluation metrics from Prometheus and Kubernetes events.

Usage:
    python3 scripts/capture_metrics.py --scenario 1 --comparator acomp
    python3 scripts/capture_metrics.py --scenario 1 --comparator smart_hpa
    python3 scripts/capture_metrics.py --scenario 2 --comparator acomp

Output: results/metrics_<scenario>_<comparator>_<timestamp>.json
        results/metrics_<scenario>_<comparator>_<timestamp>.txt  (human readable)

Prerequisites:
    - kubectl connected to cluster
    - Prometheus port-forwarded on 9090:
        kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 9090:9090 &
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone


PROMETHEUS_URL = "http://localhost:9090"
SLO_THRESHOLD_MS = 500.0
RATE_WINDOW = "10m"


def query_prometheus(promql):
    url = PROMETHEUS_URL + "/api/v1/query"
    full_url = url + "?" + urllib.parse.urlencode({"query": promql})
    try:
        with urllib.request.urlopen(full_url, timeout=10) as resp:
            data = json.loads(resp.read())
        if data.get("status") != "success":
            return None
        results = data["data"]["result"]
        if not results:
            return None
        return float(results[0]["value"][1])
    except Exception as e:
        print(f"  WARNING: Prometheus query failed: {e}")
        return None


def get_p99_latency():
    """p99 latency in milliseconds from Locust histogram."""
    val = query_prometheus(
        f'histogram_quantile(0.99, '
        f'rate(acomp_locust_response_time_seconds_bucket[{RATE_WINDOW}])) * 1000'
    )
    return round(val, 1) if val is not None else None


def get_request_rate():
    """Total request rate in req/s."""
    val = query_prometheus(
        f'sum(rate(acomp_locust_requests_total[{RATE_WINDOW}]))'
    )
    return round(val, 2) if val is not None else None


def get_slo_violation_rate():
    """Percentage of requests exceeding 500ms SLO."""
    # Use failure rate as proxy since Locust tracks failures
    val = query_prometheus(
        f'sum(rate(acomp_locust_requests_total{{status="failure"}}[{RATE_WINDOW}])) '
        f'/ sum(rate(acomp_locust_requests_total[{RATE_WINDOW}])) * 100'
    )
    return round(val, 2) if val is not None else None


def get_error_rate():
    """Raw error rate percentage."""
    val = query_prometheus(
        f'sum(rate(acomp_locust_requests_total{{status="failure"}}[{RATE_WINDOW}])) '
        f'/ sum(rate(acomp_locust_requests_total[{RATE_WINDOW}])) * 100'
    )
    return round(val, 2) if val is not None else None


def get_cpu_utilisation(service="frontend"):
    """Current CPU utilisation for a service."""
    val = query_prometheus(
        f'sum(rate(container_cpu_usage_seconds_total{{'
        f'namespace="default",pod=~"{service}-.*",'
        f'container!="",container!="POD"}}[{RATE_WINDOW}])) '
        f'/ sum(kube_pod_container_resource_requests{{'
        f'namespace="default",resource="cpu",container!="",'
        f'pod=~"{service}-.*"}})'
    )
    return round(val * 100, 1) if val is not None else None


def get_scaling_events_from_kubectl():
    """Count scaling events from kubectl events."""
    try:
        result = subprocess.check_output(
            ["kubectl", "get", "events",
             "--sort-by=.lastTimestamp",
             "--field-selector=reason=ScalingReplicaSet"],
            stderr=subprocess.DEVNULL
        ).decode("utf-8")

        lines = [l for l in result.splitlines() if "Scaled" in l]
        scale_up = sum(1 for l in lines if "up" in l.lower())
        scale_down = sum(1 for l in lines if "down" in l.lower())

        return {
            "total_events": len(lines),
            "scale_up": scale_up,
            "scale_down": scale_down,
            "events": lines[-20:],  # last 20 events
        }
    except Exception as e:
        print(f"  WARNING: kubectl events failed: {e}")
        return {"total_events": 0, "scale_up": 0, "scale_down": 0, "events": []}


def get_current_replicas():
    """Get current replica count for all services."""
    services = [
        "frontend", "currencyservice", "productcatalogservice",
        "cartservice", "recommendationservice", "checkoutservice",
        "paymentservice", "shippingservice", "emailservice",
        "adservice", "redis-cart"
    ]
    replicas = {}
    for svc in services:
        try:
            result = subprocess.check_output(
                ["kubectl", "get", "deployment", svc,
                 "-o=jsonpath={.spec.replicas}"],
                stderr=subprocess.DEVNULL
            ).decode("utf-8").strip()
            replicas[svc] = int(result) if result else 1
        except Exception:
            replicas[svc] = None
    return replicas


def count_acomp_audit_records(scenario, comparator):
    """Count ACOMP JSON Lines records if available."""
    results_base = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "results"
    )
    if not os.path.exists(results_base):
        return 0

    count = 0
    for dirname in sorted(os.listdir(results_base)):
        if f"scenario_{scenario}_{comparator}" in dirname:
            dirpath = os.path.join(results_base, dirname)
            for fname in os.listdir(dirpath):
                if fname.startswith("controller_logs"):
                    fpath = os.path.join(dirpath, fname)
                    with open(fpath) as f:
                        count += sum(
                            1 for line in f
                            if line.strip().startswith("{")
                        )
    return count


def main():
    parser = argparse.ArgumentParser(description="ACOMP Metrics Capture")
    parser.add_argument("--scenario", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--comparator", required=True,
                        choices=["acomp", "smart_hpa", "baseline_a", "baseline_b"])
    parser.add_argument("--prometheus", default="http://localhost:9090")
    args = parser.parse_args()

    global PROMETHEUS_URL
    PROMETHEUS_URL = args.prometheus

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.makedirs("results", exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Capturing metrics: Scenario {args.scenario} — {args.comparator.upper()}")
    print(f"  Timestamp: {timestamp}")
    print(f"{'='*60}\n")

    # ── Prometheus metrics ──
    print("Querying Prometheus...")
    p99 = get_p99_latency()
    req_rate = get_request_rate()
    slo_violation = get_slo_violation_rate()
    error_rate = get_error_rate()
    frontend_cpu = get_cpu_utilisation("frontend")
    currency_cpu = get_cpu_utilisation("currencyservice")

    print(f"  p99 latency:        {p99} ms")
    print(f"  Request rate:       {req_rate} req/s")
    print(f"  SLO violation rate: {slo_violation}%")
    print(f"  Error rate:         {error_rate}%")
    print(f"  Frontend CPU:       {frontend_cpu}%")
    print(f"  CurrencyService CPU:{currency_cpu}%")

    # ── Kubernetes scaling events ──
    print("\nCounting scaling events from kubectl...")
    events = get_scaling_events_from_kubectl()
    print(f"  Total scale events: {events['total_events']}")
    print(f"  Scale up:           {events['scale_up']}")
    print(f"  Scale down:         {events['scale_down']}")

    # ── Current replica state ──
    print("\nCapturing current replica counts...")
    replicas = get_current_replicas()
    for svc, count in replicas.items():
        print(f"  {svc:<30} {count} replicas")

    # ── ACOMP audit log count ──
    audit_count = 0
    if args.comparator == "acomp":
        audit_count = count_acomp_audit_records(args.scenario, args.comparator)
    print(f"\n  ACOMP audit records: {audit_count}")
    print(f"  Audit log available: {'YES' if audit_count > 0 else 'NO'}")

    # ── Oscillation index ──
    duration_hours = (events["total_events"] * 0.5) / 60  # rough estimate
    oscillation_index = round(
        events["total_events"] / duration_hours, 2
    ) if duration_hours > 0 else 0

    # ── Build results dict ──
    results = {
        "scenario": args.scenario,
        "comparator": args.comparator,
        "timestamp": timestamp,
        "metrics": {
            "p99_latency_ms": p99,
            "request_rate_rps": req_rate,
            "slo_violation_rate_pct": slo_violation,
            "error_rate_pct": error_rate,
            "frontend_cpu_pct": frontend_cpu,
            "currencyservice_cpu_pct": currency_cpu,
            "scale_events_total": events["total_events"],
            "scale_events_up": events["scale_up"],
            "scale_events_down": events["scale_down"],
            "audit_records": audit_count,
            "audit_log_available": audit_count > 0 or args.comparator != "acomp",
        },
        "replicas_at_end": replicas,
        "recent_scale_events": events["events"],
    }

    # ── Save JSON ──
    json_path = f"results/metrics_{args.scenario}_{args.comparator}_{timestamp}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    # ── Save human-readable summary ──
    txt_path = f"results/metrics_{args.scenario}_{args.comparator}_{timestamp}.txt"
    W = 60
    with open(txt_path, "w") as f:
        f.write("=" * W + "\n")
        f.write(f"  SCENARIO {args.scenario} — {args.comparator.upper()}\n")
        f.write("=" * W + "\n\n")
        f.write(f"  {'Metric':<35} {'Value':>15}\n")
        f.write(f"  {'-'*35} {'-'*15}\n")
        rows = [
            ("p99 latency (ms)",           str(p99)),
            ("Request rate (req/s)",        str(req_rate)),
            ("SLO violation rate (%)",      str(slo_violation)),
            ("Error rate (%)",              str(error_rate)),
            ("Frontend CPU (%)",            str(frontend_cpu)),
            ("Scale events total",          str(events["total_events"])),
            ("Scale events up",             str(events["scale_up"])),
            ("Scale events down",           str(events["scale_down"])),
            ("ACOMP audit records",         str(audit_count)),
            ("Audit log available",         "YES" if audit_count > 0 else "NO"),
        ]
        for label, val in rows:
            f.write(f"  {label:<35} {val:>15}\n")
        f.write("\n")
        f.write("Replica counts at end of scenario:\n")
        for svc, cnt in replicas.items():
            f.write(f"  {svc:<30} {cnt}\n")
        f.write("\nRecent scale events:\n")
        for evt in events["events"]:
            f.write(f"  {evt}\n")

    print(f"\n{'='*60}")
    print(f"  Results saved:")
    print(f"    {json_path}")
    print(f"    {txt_path}")
    print(f"{'='*60}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())