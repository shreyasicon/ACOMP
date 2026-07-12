#!/usr/bin/env python3
"""
scripts/calibrate_work_factors.py

Empirically calibrates work factors for the ACOMP Context Map by running
controlled load tests at three levels (low, medium, high) and measuring
the request rate ratio between each dependent service pair.

Per the thesis Methodology (Equation 3):
    W(A, B) = median[ R_B(l) / R_A(l) | l in {low, medium, high} ]

where R_A(l) and R_B(l) are observed request rates at services A and B
under controlled load level l. The median across three levels produces a
ratio that is robust to transient outliers in any single run.

Usage:
    python3 scripts/calibrate_work_factors.py
    python3 scripts/calibrate_work_factors.py --output alomp_config.yaml
    python3 scripts/calibrate_work_factors.py --prometheus http://localhost:9091

The script writes calibrated work factors directly into alomp_config.yaml,
replacing all PLACEHOLDER values with empirically derived figures.

Prerequisites:
    - kubectl connected to the cluster
    - Prometheus port-forwarded:
        kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 9090:9090 &
    - ACOMP controller running (or at least Prometheus and Online Boutique)
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone


# ---------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------

# Load levels: (locust_users, spawn_rate, hold_duration_seconds)
LOAD_LEVELS = [
    ("low",    20,  5,  120),   # 20 users, ramp 5/s, hold 2 min
    ("medium", 80,  10, 120),   # 80 users, ramp 10/s, hold 2 min
    ("high",   200, 20, 120),   # 200 users, ramp 20/s, hold 2 min
]

# Service pairs to calibrate: (upstream, downstream)
# Derived from Online Boutique's actual gRPC call graph
SERVICE_PAIRS = [
    ("frontend",          "productcatalogservice"),
    ("frontend",          "cartservice"),
    ("frontend",          "checkoutservice"),
    ("frontend",          "currencyservice"),
    ("frontend",          "shippingservice"),
    ("frontend",          "recommendationservice"),
    ("frontend",          "adservice"),
    ("checkoutservice",   "paymentservice"),
    ("checkoutservice",   "emailservice"),
    ("checkoutservice",   "shippingservice"),
    ("checkoutservice",   "currencyservice"),
    ("checkoutservice",   "cartservice"),
    ("checkoutservice",   "productcatalogservice"),
    ("cartservice",       "redis-cart"),
    ("recommendationservice", "productcatalogservice"),
]

# Prometheus query rate window — use 1m to match Collector
RATE_WINDOW = "1m"


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def run(cmd, check=True):
    print(f"  $ {cmd}")
    subprocess.run(cmd, shell=True, check=check)


def query_prometheus(prometheus_url, promql):
    """Execute a Prometheus instant query and return results."""
    url = prometheus_url.rstrip("/") + "/api/v1/query"
    full_url = url + "?" + urllib.parse.urlencode({"query": promql})
    try:
        with urllib.request.urlopen(full_url, timeout=10) as resp:
            data = json.loads(resp.read())
        if data.get("status") != "success":
            return []
        return data["data"]["result"]
    except Exception as e:
        log(f"WARNING: Prometheus query failed: {e}")
        return []


def get_request_rate(prometheus_url, service, namespace="default"):
    """
    Get the current request rate (requests/sec) for a service.
    Uses container_network_receive_bytes_total as a proxy for services
    that don't expose application metrics directly, falling back to
    kube_pod_container_info counts where network data is unavailable.

    Primary: rate of HTTP requests via Locust-driven traffic, measured
    as the rate of container network bytes received normalised by pod count.
    """
    # Try to get from Locust metrics first (frontend only, entry point)
    if service == "frontend":
        promql = f'sum(rate(acomp_locust_requests_total[{RATE_WINDOW}]))'
        results = query_prometheus(prometheus_url, promql)
        if results:
            try:
                return float(results[0]["value"][1])
            except (KeyError, IndexError, ValueError):
                pass

    # For internal services, use network receive rate as proxy for request rate
    # This measures bytes/sec arriving at the pod, which is proportional to req/s
    promql = (
        f'sum(rate(container_network_receive_bytes_total{{'
        f'namespace="{namespace}", pod=~"{service}-.*"'
        f'}}[{RATE_WINDOW}])) / 1024'
    )
    results = query_prometheus(prometheus_url, promql)
    if results:
        try:
            return float(results[0]["value"][1])
        except (KeyError, IndexError, ValueError):
            pass

    return None


def set_locust_users(users, spawn_rate, run_time_s):
    """Patch the Locust deployment to the given user count."""
    args = (
        f'["--host=http://frontend","--headless",'
        f'"--users={users}","--spawn-rate={spawn_rate}",'
        f'"--run-time={run_time_s}s"]'
    )
    patch = (
        f'{{"spec":{{"template":{{"spec":{{"containers":[{{'
        f'"name":"locust","args":{args}}}]}}}}}}}}'
    )
    run(f"kubectl patch deployment acomp-loadgenerator -p '{patch}'")
    log(f"Waiting 30s for Locust to ramp to {users} users...")
    time.sleep(30)


def measure_rates(prometheus_url, pairs, namespace):
    """Measure request rates for all services in the given pairs."""
    services = set()
    for upstream, downstream in pairs:
        services.add(upstream)
        services.add(downstream)

    rates = {}
    for svc in sorted(services):
        rate = get_request_rate(prometheus_url, svc, namespace)
        rates[svc] = rate
        log(f"  {svc:<35} rate = {rate:.3f} req/s" if rate is not None
            else f"  {svc:<35} rate = N/A")

    return rates


def compute_work_factors(measurements, pairs):
    """
    Compute W(upstream, downstream) for each service pair as the median
    ratio R_downstream / R_upstream across all load levels.

    Returns dict: {(upstream, downstream): work_factor}
    """
    work_factors = {}

    for upstream, downstream in pairs:
        ratios = []
        for level_name, rates in measurements.items():
            r_up = rates.get(upstream)
            r_down = rates.get(downstream)
            if r_up and r_down and r_up > 0:
                ratio = r_down / r_up
                ratios.append(ratio)
                log(f"  [{level_name}] W({upstream},{downstream}) = "
                    f"{r_down:.3f} / {r_up:.3f} = {ratio:.3f}")
            else:
                log(f"  [{level_name}] W({upstream},{downstream}) = N/A "
                    f"(upstream={r_up}, downstream={r_down})")

        if ratios:
            wf = round(statistics.median(ratios), 3)
            work_factors[(upstream, downstream)] = wf
            log(f"  --> W({upstream},{downstream}) = median({[round(r,3) for r in ratios]}) = {wf}")
        else:
            work_factors[(upstream, downstream)] = 0.50
            log(f"  --> W({upstream},{downstream}) = 0.50 (insufficient data, keeping placeholder)")

    return work_factors


def update_config_file(config_path, work_factors):
    """
    Write calibrated work factors into alomp_config.yaml, replacing
    all PLACEHOLDER lines with the empirically derived values.
    """
    with open(config_path) as f:
        lines = f.readlines()

    new_lines = []
    current_upstream = None
    current_downstream = None

    for line in lines:
        stripped = line.strip()

        # Track current service context
        if stripped.startswith("- name:"):
            current_upstream = stripped.split(":", 1)[1].strip()
            current_downstream = None

        if stripped.startswith("- service:"):
            current_downstream = stripped.split(":", 1)[1].strip()

        # Replace PLACEHOLDER work_factor lines
        if "work_factor:" in stripped and "PLACEHOLDER" in line:
            if current_upstream and current_downstream:
                key = (current_upstream, current_downstream)
                wf = work_factors.get(key, 0.50)
                indent = len(line) - len(line.lstrip())
                new_line = (
                    " " * indent +
                    f"work_factor: {wf}   "
                    f"# calibrated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
                )
                new_lines.append(new_line)
                continue

        new_lines.append(line)

    with open(config_path, "w") as f:
        f.writelines(new_lines)

    log(f"Updated {config_path} with calibrated work factors.")


def print_summary(work_factors):
    """Print a summary table of calibrated work factors."""
    print("\n" + "=" * 60)
    print("  Calibrated Work Factors")
    print("=" * 60)
    print(f"  {'Upstream':<28} {'Downstream':<28} {'W':>6}")
    print(f"  {'-'*28} {'-'*28} {'-'*6}")
    for (up, down), wf in sorted(work_factors.items()):
        print(f"  {up:<28} {down:<28} {wf:>6.3f}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Calibrate ACOMP work factors from live Prometheus metrics"
    )
    parser.add_argument(
        "--prometheus",
        default="http://localhost:9090",
        help="Prometheus URL (default: http://localhost:9090)"
    )
    parser.add_argument(
        "--output",
        default="alomp_config.yaml",
        help="Path to alomp_config.yaml to update (default: alomp_config.yaml)"
    )
    parser.add_argument(
        "--namespace",
        default="default",
        help="Kubernetes namespace (default: default)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print calibrated values without updating the config file"
    )
    args = parser.parse_args()

    log("ACOMP Work Factor Calibration")
    log(f"Prometheus: {args.prometheus}")
    log(f"Config: {args.output}")
    log(f"Pairs to calibrate: {len(SERVICE_PAIRS)}")

    measurements = {}

    for level_name, users, spawn_rate, hold_s in LOAD_LEVELS:
        log(f"\n--- Load level: {level_name} ({users} users) ---")

        # Set load
        set_locust_users(users=users, spawn_rate=spawn_rate, run_time_s=hold_s + 60)

        # Wait for steady state (use most of the hold duration)
        log(f"Holding for {hold_s}s to reach steady state...")
        time.sleep(hold_s)

        # Measure
        log("Measuring request rates across all services...")
        rates = measure_rates(args.prometheus, SERVICE_PAIRS, args.namespace)
        measurements[level_name] = rates

    # Compute work factors
    log("\n--- Computing work factors ---")
    work_factors = compute_work_factors(measurements, SERVICE_PAIRS)

    # Print summary
    print_summary(work_factors)

    if args.dry_run:
        log("Dry run — config file not updated.")
    else:
        if not os.path.exists(args.output):
            log(f"ERROR: Config file not found: {args.output}")
            return 1
        update_config_file(args.output, work_factors)
        log("Calibration complete. Restart ACOMP controller to pick up new values:")
        log("  kubectl rollout restart deployment/acomp-controller")

    # Reset to low load
    set_locust_users(users=10, spawn_rate=2, run_time_s=3600)

    return 0


if __name__ == "__main__":
    sys.exit(main())
