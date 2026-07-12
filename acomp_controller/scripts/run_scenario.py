#!/usr/bin/env python3
"""
scripts/run_scenario.py

Automated scenario runner for ACOMP evaluation.
Runs one of three controlled scenarios, captures all relevant logs
and metrics, and saves results to a timestamped directory.

Usage:
    python3 run_scenario.py --scenario 1 --comparator acomp
    python3 run_scenario.py --scenario 2 --comparator baseline_a
    python3 run_scenario.py --scenario 3 --comparator baseline_b

Comparators:
    acomp       -- ACOMP controller active, HPA disabled
    baseline_a  -- Standard Kubernetes HPA only (no ACOMP)
    baseline_b  -- Uncoordinated HPA + VPA (no ACOMP)

Results are saved to: results/scenario_<N>_<comparator>_<timestamp>/
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone


# ---------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------

SERVICES = [
    "frontend",
    "currencyservice",
    "productcatalogservice",
    "cartservice",
    "recommendationservice",
    "checkoutservice",
    "paymentservice",
    "shippingservice",
    "emailservice",
    "adservice",
]

SCENARIO_CONFIG = {
    1: {
        "name": "Steady to Bursty Load",
        "description": "Ramp from 10 to 200 users over 5 minutes, hold 10 minutes",
        "phases": [
            {"users": 10,  "spawn_rate": 2,  "duration_s": 120, "label": "baseline"},
            {"users": 200, "spawn_rate": 50, "duration_s": 600, "label": "bursty"},
            {"users": 10,  "spawn_rate": 10, "duration_s": 120, "label": "recovery"},
        ],
    },
    2: {
        "name": "Sustained High-Pressure Load",
        "description": "Hold 200 users continuously for 30 minutes",
        "phases": [
            {"users": 10,  "spawn_rate": 2,  "duration_s": 60,   "label": "baseline"},
            {"users": 200, "spawn_rate": 50, "duration_s": 1800, "label": "sustained"},
        ],
    },
    3: {
        "name": "Downstream Degradation Injection",
        "description": "50 users + inject 3s latency into recommendationservice",
        "phases": [
            {"users": 50, "spawn_rate": 5, "duration_s": 120, "label": "baseline"},
            {"users": 50, "spawn_rate": 5, "duration_s": 600, "label": "degradation",
             "inject_latency": {"service": "recommendationservice", "millis": 3000}},
            {"users": 50, "spawn_rate": 5, "duration_s": 120, "label": "recovery",
             "remove_latency": {"service": "recommendationservice"}},
        ],
    },
    # ── General / Edge Case Scenarios ─────────────────────────────

    4: {
        "name": "Pipeline Ceiling (Guardrail Hit)",
        "description": "Push load until services hit max_replicas. ACOMP classifies PIPELINE_CEILING.",
        "phases": [
            {"users": 10,  "spawn_rate": 2,   "duration_s": 60,  "label": "baseline"},
            {"users": 500, "spawn_rate": 100,  "duration_s": 600, "label": "ceiling_load"},
            {"users": 10,  "spawn_rate": 10,   "duration_s": 120, "label": "recovery"},
        ],
    },

    5: {
        "name": "Unable to Scale - RBAC Error Simulation",
        "description": "Revoke ACOMP RBAC mid-run. Expects FAILED actuation entries, no crash.",
        "phases": [
            {"users": 100, "spawn_rate": 10, "duration_s": 120, "label": "normal"},
            {"users": 100, "spawn_rate": 10, "duration_s": 180, "label": "rbac_revoked",
             "revoke_rbac": True},
            {"users": 100, "spawn_rate": 10, "duration_s": 120, "label": "rbac_restored",
             "restore_rbac": True},
        ],
    },

    6: {
        "name": "Prometheus Unavailable - Collector Resilience",
        "description": "Kill Prometheus. Collector should skip cycles gracefully and resume.",
        "phases": [
            {"users": 50, "spawn_rate": 5, "duration_s": 60,  "label": "normal"},
            {"users": 50, "spawn_rate": 5, "duration_s": 120, "label": "prometheus_down",
             "kill_prometheus": True},
            {"users": 50, "spawn_rate": 5, "duration_s": 120, "label": "prometheus_restored",
             "restore_prometheus": True},
        ],
    },

    7: {
        "name": "Rapid Load Oscillation - Anti-Thrash Behaviour",
        "description": "Alternate high/low load every 30s. ACOMP should keep oscillation index low.",
        "phases": [
            {"users": 10,  "spawn_rate": 5,  "duration_s": 30, "label": "low_1"},
            {"users": 200, "spawn_rate": 50, "duration_s": 30, "label": "high_1"},
            {"users": 10,  "spawn_rate": 5,  "duration_s": 30, "label": "low_2"},
            {"users": 200, "spawn_rate": 50, "duration_s": 30, "label": "high_2"},
            {"users": 10,  "spawn_rate": 5,  "duration_s": 30, "label": "low_3"},
            {"users": 200, "spawn_rate": 50, "duration_s": 30, "label": "high_3"},
            {"users": 10,  "spawn_rate": 5,  "duration_s": 60, "label": "cooldown"},
        ],
    },

    8: {
        "name": "Controller Cold Start - Restart Mid-Load",
        "description": "Restart ACOMP pod under sustained load. Should resume correct decisions immediately.",
        "phases": [
            {"users": 150, "spawn_rate": 20, "duration_s": 120, "label": "pre_restart"},
            {"users": 150, "spawn_rate": 20, "duration_s": 180, "label": "post_restart",
             "restart_controller": True},
        ],
    },
}


# ---------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------

def run(cmd, check=True, capture=False):
    """Run a shell command."""
    print(f"  $ {cmd}")
    result = subprocess.run(
        cmd, shell=True, check=check,
        capture_output=capture, text=True
    )
    return result


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def reset_replicas(services):
    """Scale all services back to 1 replica for a clean baseline."""
    log("Resetting all services to 1 replica...")
    for svc in services:
        run(f"kubectl scale deployment {svc} --replicas=1", check=False)
    time.sleep(30)
    log("Waiting for pods to stabilise...")
    run("kubectl wait --for=condition=available deployment --all --timeout=120s", check=False)


def set_locust_users(users, spawn_rate, run_time_s):
    """Patch the Locust deployment to use given user count."""
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
    time.sleep(15)  # Allow pod to restart


def inject_latency(service, millis):
    """Inject artificial latency into a service via environment variable."""
    log(f"Injecting {millis}ms latency into {service}...")
    run(f"kubectl set env deployment/{service} EXTRA_LATENCY_MILLIS={millis}")
    time.sleep(10)


def remove_latency(service):
    """Remove injected latency from a service."""
    log(f"Removing latency injection from {service}...")
    run(f"kubectl set env deployment/{service} EXTRA_LATENCY_MILLIS-", check=False)
    time.sleep(10)


def capture_controller_logs(results_dir, label):
    """Save current ACOMP controller logs to results directory."""
    log(f"Capturing controller logs ({label})...")
    result = run(
        "kubectl logs -l app=acomp-controller --tail=500",
        capture=True, check=False
    )
    path = os.path.join(results_dir, f"controller_logs_{label}.txt")
    with open(path, "w") as f:
        f.write(result.stdout)
    return path


def capture_metrics_snapshot(results_dir, label):
    """Save kubectl top pods output to results directory."""
    result = run("kubectl top pods", capture=True, check=False)
    path = os.path.join(results_dir, f"pod_metrics_{label}.txt")
    with open(path, "w") as f:
        f.write(result.stdout)
    return path


def capture_locust_stats(results_dir, label):
    """Save Locust stats from its logs."""
    result = run(
        "kubectl logs -l app=acomp-loadgenerator --tail=20",
        capture=True, check=False
    )
    path = os.path.join(results_dir, f"locust_stats_{label}.txt")
    with open(path, "w") as f:
        f.write(result.stdout)
    return path


def disable_acomp():
    """Scale ACOMP controller to 0 replicas (for baseline runs)."""
    log("Disabling ACOMP controller (baseline mode)...")
    run("kubectl scale deployment acomp-controller --replicas=0")


def enable_acomp():
    """Scale ACOMP controller back to 1 replica."""
    log("Enabling ACOMP controller...")
    run("kubectl scale deployment acomp-controller --replicas=1")
    time.sleep(20)


def enable_hpa(services, cpu_threshold=70):
    """Create HPA objects for each service (Baseline A)."""
    log(f"Creating HPA for all services at {cpu_threshold}% CPU threshold...")
    for svc in services:
        run(
            f"kubectl autoscale deployment {svc} "
            f"--cpu-percent={cpu_threshold} --min=1 --max=10",
            check=False
        )


def disable_hpa(services):
    """Delete all HPA objects."""
    log("Deleting all HPA objects...")
    for svc in services:
        run(f"kubectl delete hpa {svc}", check=False)



def revoke_rbac():
    log("Revoking ACOMP ClusterRoleBinding (simulating permission failure)...")
    run("kubectl delete clusterrolebinding acomp-controller", check=False)


def restore_rbac():
    log("Restoring ACOMP ClusterRoleBinding...")
    run(
        "kubectl create clusterrolebinding acomp-controller "
        "--clusterrole=acomp-controller "
        "--serviceaccount=default:acomp-controller",
        check=False
    )
    time.sleep(10)


def kill_prometheus():
    log("Scaling Prometheus to 0 (simulating metrics outage)...")
    run("kubectl scale statefulset prometheus-prometheus-kube-prometheus-prometheus --replicas=0 -n monitoring", check=False)
    time.sleep(20)


def restore_prometheus():
    log("Restoring Prometheus...")
    run("kubectl scale statefulset prometheus-prometheus-kube-prometheus-prometheus --replicas=1 -n monitoring", check=False)
    time.sleep(30)
    run("kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus -n monitoring --timeout=120s", check=False)


def restart_controller():
    log("Restarting ACOMP controller pod mid-scenario...")
    run("kubectl rollout restart deployment/acomp-controller")
    time.sleep(20)
    run("kubectl wait --for=condition=available deployment/acomp-controller --timeout=60s", check=False)
    log("ACOMP controller back online.")


# ---------------------------------------------------------------
# Results analysis
# ---------------------------------------------------------------

def analyse_controller_logs(log_file):
    """Parse ACOMP JSON Lines log file and compute metrics."""
    records = []
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("{"):
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except FileNotFoundError:
        return {}

    if not records:
        return {}

    total_cycles = len(records)
    state_counts = {}
    applied = 0
    skipped = 0
    failed = 0

    for r in records:
        state = r.get("pipeline_state", "UNKNOWN")
        state_counts[state] = state_counts.get(state, 0) + 1
        summary = r.get("actuation_summary", {})
        applied += summary.get("applied", 0)
        skipped += summary.get("skipped", 0)
        failed += summary.get("failed", 0)

    duration_hours = (total_cycles * 15) / 3600
    oscillation_index = applied / duration_hours if duration_hours > 0 else 0

    return {
        "total_cycles": total_cycles,
        "duration_minutes": round(total_cycles * 15 / 60, 1),
        "state_distribution": state_counts,
        "actuation": {
            "applied": applied,
            "skipped": skipped,
            "failed": failed,
        },
        "oscillation_index_per_hour": round(oscillation_index, 2),
    }


def save_summary(results_dir, scenario_num, comparator, phase_results):
    """Write a human-readable summary file."""
    summary = {
        "scenario": scenario_num,
        "scenario_name": SCENARIO_CONFIG[scenario_num]["name"],
        "comparator": comparator,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "phases": phase_results,
    }
    path = os.path.join(results_dir, "summary.json")
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)

    # Also write human-readable version
    txt_path = os.path.join(results_dir, "summary.txt")
    with open(txt_path, "w") as f:
        f.write(f"ACOMP Evaluation Results\n")
        f.write(f"========================\n")
        f.write(f"Scenario {scenario_num}: {SCENARIO_CONFIG[scenario_num]['name']}\n")
        f.write(f"Comparator: {comparator}\n")
        f.write(f"Timestamp: {summary['timestamp']}\n\n")
        for phase in phase_results:
            f.write(f"Phase: {phase.get('label', '?')}\n")
            metrics = phase.get("controller_metrics", {})
            if metrics:
                f.write(f"  Cycles: {metrics.get('total_cycles', 0)}\n")
                f.write(f"  Duration: {metrics.get('duration_minutes', 0)} min\n")
                f.write(f"  States: {metrics.get('state_distribution', {})}\n")
                f.write(f"  Applied: {metrics.get('actuation', {}).get('applied', 0)}\n")
                f.write(f"  Skipped: {metrics.get('actuation', {}).get('skipped', 0)}\n")
                f.write(f"  Oscillation index: {metrics.get('oscillation_index_per_hour', 0)} events/hr\n")
            f.write("\n")

    log(f"Summary saved to {txt_path}")
    return txt_path


# ---------------------------------------------------------------
# Main scenario runner
# ---------------------------------------------------------------

def run_scenario(scenario_num, comparator):
    config = SCENARIO_CONFIG[scenario_num]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = f"results/scenario_{scenario_num}_{comparator}_{timestamp}"
    os.makedirs(results_dir, exist_ok=True)

    log(f"Starting Scenario {scenario_num}: {config['name']}")
    log(f"Comparator: {comparator}")
    log(f"Results directory: {results_dir}")

    # ── Setup comparator ──
    if comparator == "acomp":
        disable_hpa(SERVICES)
        enable_acomp()
    elif comparator == "baseline_a":
        disable_acomp()
        disable_hpa(SERVICES)
        enable_hpa(SERVICES, cpu_threshold=70)
    elif comparator == "baseline_b":
        disable_acomp()
        disable_hpa(SERVICES)
        enable_hpa(SERVICES, cpu_threshold=70)
        # VPA would be set up here if installed
        log("Note: VPA setup requires VPA operator installed separately")

    # ── Reset to clean baseline ──
    reset_replicas(SERVICES)
    capture_metrics_snapshot(results_dir, "pre_scenario")

    # ── Run phases ──
    phase_results = []
    for i, phase in enumerate(config["phases"]):
        label = phase["label"]
        log(f"\n--- Phase {i+1}/{len(config['phases'])}: {label} ---")

        # Apply load
        set_locust_users(
            users=phase["users"],
            spawn_rate=phase["spawn_rate"],
            run_time_s=phase["duration_s"] + 30,
        )

        # Inject latency if specified
        if "inject_latency" in phase:
            inject_latency(
                phase["inject_latency"]["service"],
                phase["inject_latency"]["millis"]
            )

        # Remove latency if specified
        if "remove_latency" in phase:
            remove_latency(phase["remove_latency"]["service"])

        # RBAC revocation/restoration (Scenario 5)
        if phase.get("revoke_rbac"):
            revoke_rbac()
        if phase.get("restore_rbac"):
            restore_rbac()

        # Prometheus kill/restore (Scenario 6)
        if phase.get("kill_prometheus"):
            kill_prometheus()
        if phase.get("restore_prometheus"):
            restore_prometheus()

        # Controller restart (Scenario 8)
        if phase.get("restart_controller"):
            restart_controller()

        # Wait for phase duration
        log(f"Running phase '{label}' for {phase['duration_s']}s...")
        time.sleep(phase["duration_s"])

        # Capture results at end of phase
        log_file = capture_controller_logs(results_dir, f"{i+1}_{label}")
        capture_metrics_snapshot(results_dir, f"{i+1}_{label}")
        capture_locust_stats(results_dir, f"{i+1}_{label}")

        metrics = analyse_controller_logs(log_file)
        phase_results.append({"label": label, "controller_metrics": metrics})

        log(f"Phase '{label}' complete. Metrics: {metrics}")

    # ── Cleanup ──
    log("\nCleaning up...")
    # Remove any injected latency
    for phase in config["phases"]:
        if "inject_latency" in phase:
            remove_latency(phase["inject_latency"]["service"])

    # Reset Locust to low load
    set_locust_users(users=10, spawn_rate=2, run_time_s=3600)

    # Always re-enable ACOMP at end
    enable_acomp()

    # ── Save summary ──
    save_summary(results_dir, scenario_num, comparator, phase_results)

    log(f"\nScenario {scenario_num} complete. Results in: {results_dir}")
    return results_dir


# ---------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ACOMP Scenario Runner")
    parser.add_argument("--scenario", type=int, choices=[1, 2, 3, 4, 5, 6, 7, 8], required=True)
    parser.add_argument(
        "--comparator", choices=["acomp", "baseline_a", "baseline_b"],
        default="acomp"
    )
    args = parser.parse_args()

    try:
        results_dir = run_scenario(args.scenario, args.comparator)
        print(f"\nDone. Results: {results_dir}")
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 1
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    sys.exit(main())