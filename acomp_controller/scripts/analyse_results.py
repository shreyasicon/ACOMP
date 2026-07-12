#!/usr/bin/env python3
"""
scripts/analyse_results.py

Reads all captured scenario result directories and produces a comparison
table across all three comparators (ACOMP, Baseline A, Baseline B) for
each scenario. Output matches the evaluation tables in the thesis.

Usage:
    python3 analyse_results.py
    python3 analyse_results.py --results-dir results/
    python3 analyse_results.py --scenario 1
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict


SLO_THRESHOLD_MS = 500.0  # p99 latency SLO from thesis


def find_result_dirs(base_dir="results"):
    """Find all scenario result directories."""
    if not os.path.exists(base_dir):
        print(f"No results directory found at: {base_dir}")
        return []
    dirs = []
    for name in sorted(os.listdir(base_dir)):
        path = os.path.join(base_dir, name)
        if os.path.isdir(path) and name.startswith("scenario_"):
            summary = os.path.join(path, "summary.json")
            if os.path.exists(summary):
                dirs.append(path)
    return dirs


def load_summary(results_dir):
    """Load the summary.json from a results directory."""
    path = os.path.join(results_dir, "summary.json")
    with open(path) as f:
        return json.load(f)


def parse_locust_stats(locust_file):
    """
    Parse Locust stats file to extract p99 latency, request rate, error rate.
    Locust outputs a table like:
      GET /product/[id]  3150  0(0.00%)  86  10  2154  37  23.40  0.00
    The Aggregated row has the overall stats.
    """
    stats = {"p99_latency_ms": None, "request_rate": None, "error_rate": None}
    try:
        with open(locust_file) as f:
            content = f.read()

        # Find the Aggregated row
        for line in content.split("\n"):
            if "Aggregated" in line:
                # Extract numbers from the line
                nums = re.findall(r"[\d.]+", line)
                if len(nums) >= 7:
                    # Locust format: reqs fails% avg min max med req/s fail/s
                    # Column positions vary slightly, use last few
                    stats["request_rate"] = float(nums[-2])
                    # p99 is not directly in this output -- use Max as proxy
                    # (Locust --headless doesn't output percentiles in table mode)
                    stats["p99_latency_ms"] = float(nums[-5]) if len(nums) >= 5 else None
                break
    except (FileNotFoundError, ValueError, IndexError):
        pass
    return stats


def parse_controller_json_logs(log_file):
    """Parse ACOMP JSON Lines controller log file."""
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

    total = len(records)
    states = defaultdict(int)
    applied = skipped = failed = 0
    downstream_degradation_cycles = 0

    for r in records:
        state = r.get("pipeline_state", "UNKNOWN")
        states[state] += 1
        s = r.get("actuation_summary", {})
        applied += s.get("applied", 0)
        skipped += s.get("skipped", 0)
        failed += s.get("failed", 0)
        if state == "DOWNSTREAM_DEGRADATION":
            downstream_degradation_cycles += 1

    duration_hours = (total * 15) / 3600
    oscillation_index = round(applied / duration_hours, 2) if duration_hours > 0 else 0

    return {
        "total_cycles": total,
        "duration_min": round(total * 15 / 60, 1),
        "states": dict(states),
        "applied": applied,
        "skipped": skipped,
        "failed": failed,
        "oscillation_index": oscillation_index,
        "downstream_suppressed": downstream_degradation_cycles,
        "healthy_pct": round(states.get("HEALTHY", 0) / total * 100, 1) if total else 0,
    }


def print_comparison_table(scenario_num, results_by_comparator):
    """Print a formatted comparison table for one scenario."""
    comparators = ["acomp", "baseline_a", "baseline_b"]
    labels = {
        "acomp": "ACOMP",
        "baseline_a": "Baseline A (HPA)",
        "baseline_b": "Baseline B (HPA+VPA)",
    }

    print(f"\n{'='*70}")
    print(f"  Scenario {scenario_num} Results")
    print(f"{'='*70}")
    print(f"{'Metric':<35} {'ACOMP':>10} {'Baseline A':>12} {'Baseline B':>12}")
    print(f"{'-'*70}")

    def val(comparator, key, default="N/A"):
        data = results_by_comparator.get(comparator, {})
        v = data.get(key, default)
        return str(v) if v != default else default

    metrics = [
        ("Total cycles", "total_cycles"),
        ("Duration (min)", "duration_min"),
        ("Applied scale events", "applied"),
        ("Skipped (idempotent)", "skipped"),
        ("Oscillation index (events/hr)", "oscillation_index"),
        ("HEALTHY cycles (%)", "healthy_pct"),
        ("Downstream suppressed cycles", "downstream_suppressed"),
        ("Failed actuations", "failed"),
    ]

    for label, key in metrics:
        row = f"{label:<35}"
        for comp in comparators:
            row += f" {val(comp, key):>12}"
        print(row)

    print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description="ACOMP Results Analyser")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--scenario", type=int, choices=[1, 2, 3])
    args = parser.parse_args()

    dirs = find_result_dirs(args.results_dir)
    if not dirs:
        print("No result directories found. Run run_scenario.py first.")
        return 1

    # Group by scenario number
    by_scenario = defaultdict(dict)
    for d in dirs:
        name = os.path.basename(d)
        # Parse scenario_N_comparator_timestamp
        parts = name.split("_")
        if len(parts) < 3:
            continue
        try:
            scenario_num = int(parts[1])
            comparator = parts[2]
        except (ValueError, IndexError):
            continue

        if args.scenario and scenario_num != args.scenario:
            continue

        summary = load_summary(d)

        # Aggregate controller metrics across all phases
        all_records_file = os.path.join(
            d, f"controller_logs_2_bursty.txt"
        ) if scenario_num == 1 else os.path.join(
            d, f"controller_logs_2_sustained.txt"
        ) if scenario_num == 2 else os.path.join(
            d, f"controller_logs_2_degradation.txt"
        )

        # Fall back to any controller log file
        if not os.path.exists(all_records_file):
            for fname in sorted(os.listdir(d)):
                if fname.startswith("controller_logs"):
                    all_records_file = os.path.join(d, fname)
                    break

        metrics = parse_controller_json_logs(all_records_file)
        by_scenario[scenario_num][comparator] = metrics

    if not by_scenario:
        print("No results to display.")
        return 1

    for scenario_num in sorted(by_scenario.keys()):
        print_comparison_table(scenario_num, by_scenario[scenario_num])

    return 0


if __name__ == "__main__":
    sys.exit(main())
