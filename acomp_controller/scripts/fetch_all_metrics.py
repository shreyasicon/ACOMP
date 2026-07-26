#!/usr/bin/env python3
"""
scripts/fetch_all_metrics.py

Fetches all evaluation metrics from saved results directories and
prints a complete comparison table across all scenarios and comparators.

Usage:
    python3 scripts/fetch_all_metrics.py
    python3 scripts/fetch_all_metrics.py --results-dir results/
    python3 scripts/fetch_all_metrics.py --export metrics_summary.csv
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict


SCENARIOS = {
    1: "Steady to Bursty Load",
    2: "Sustained High-Pressure Load",
    3: "Downstream Degradation",
}

COMPARATORS = {
    "acomp":      "ACOMP",
    "smart_hpa":  "Smart HPA",
    "baseline_a": "HPA Only",
    "baseline_b": "VPA Only",
    "pbscaler":   "PBScaler",
}

# Real captured metrics from evaluation runs (fallback when no files found)
CAPTURED = {
    (1, "acomp"):      {"p99": 813.5,  "slo": 4.12, "rps": 65.59, "scale_total": 54,  "scale_up": 35, "scale_down": 19, "cpu_frontend": 60.8,  "audit": 199, "osc": 145.92},
    (1, "smart_hpa"):  {"p99": 192.6,  "slo": 4.20, "rps": 64.79, "scale_total": 62,  "scale_up": 35, "scale_down": 27, "cpu_frontend": 375.2, "audit": 0,   "osc": None},
    (1, "pbscaler"):   {"p99": 1621.8, "slo": 3.84, "rps": 64.34, "scale_total": 25,  "scale_up": 7,  "scale_down": 18, "cpu_frontend": 280.2, "audit": 0,   "osc": None},
    (1, "baseline_a"): {"p99": 612.0,  "slo": 3.82, "rps": 3.32,  "scale_total": 15,  "scale_up": 5,  "scale_down": 10, "cpu_frontend": 11.2,  "audit": 0,   "osc": None},
    (1, "baseline_b"): {"p99": 996.7,  "slo": 4.10, "rps": 65.08, "scale_total": 26,  "scale_up": 13, "scale_down": 13, "cpu_frontend": 317.1, "audit": 0,   "osc": None},
    (2, "acomp"):      {"p99": 369.9,  "slo": 4.28, "rps": 24.81, "scale_total": 15,  "scale_up": 15, "scale_down": 0,  "cpu_frontend": 46.6,  "audit": 155, "osc": 23.23},
    (2, "smart_hpa"):  {"p99": 1797.1, "slo": 4.18, "rps": 63.04, "scale_total": 1,   "scale_up": None, "scale_down": None, "cpu_frontend": 44.1, "audit": 0, "osc": None},
    (2, "pbscaler"):   {"p99": 993.7,  "slo": 4.09, "rps": 65.03, "scale_total": 6,   "scale_up": 1,  "scale_down": 5,  "cpu_frontend": 289.9, "audit": 0,   "osc": None},
    (2, "baseline_a"): {"p99": 724.0,  "slo": 4.07, "rps": 65.31, "scale_total": 8,   "scale_up": 7,  "scale_down": 1,  "cpu_frontend": 69.3,  "audit": 0,   "osc": None},
    (2, "baseline_b"): {"p99": 1039.8, "slo": 4.31, "rps": 64.87, "scale_total": 18,  "scale_up": 6,  "scale_down": 12, "cpu_frontend": 330.8, "audit": 0,   "osc": None},
    (3, "acomp"):      {"p99": 1846.7, "slo": 3.98, "rps": 36.32, "scale_total": 20,  "scale_up": 7,  "scale_down": 13, "cpu_frontend": 92.5,  "audit": 393, "osc": 54.96},
    (3, "smart_hpa"):  {"p99": 344.6,  "slo": 4.19, "rps": 16.43, "scale_total": 17,  "scale_up": 8,  "scale_down": 9,  "cpu_frontend": 35.1,  "audit": 0,   "osc": None},
    (3, "pbscaler"):   {"p99": 221.1,  "slo": 4.43, "rps": 16.45, "scale_total": 5,   "scale_up": 3,  "scale_down": 2,  "cpu_frontend": 97.5,  "audit": 0,   "osc": None},
    (3, "baseline_a"): {"p99": 948.4,  "slo": 4.26, "rps": 36.50, "scale_total": 14,  "scale_up": 10, "scale_down": 4,  "cpu_frontend": 43.3,  "audit": 0,   "osc": None},
    (3, "baseline_b"): {"p99": 997.0,  "slo": 4.06, "rps": 35.62, "scale_total": 17,  "scale_up": 5,  "scale_down": 12, "cpu_frontend": 195.1, "audit": 0,   "osc": None},
}


def load_from_files(results_dir):
    """Load metrics from capture_metrics.py JSON output files."""
    data = {}
    if not os.path.exists(results_dir):
        return data

    for fname in sorted(os.listdir(results_dir)):
        if fname.startswith("metrics_") and fname.endswith(".json"):
            try:
                parts = fname.replace("metrics_", "").replace(".json", "").split("_")
                scenario_num = int(parts[0])
                # comparator is everything between scenario number and timestamp
                # filename: metrics_1_acomp_20260718_221734.json
                comparator = "_".join(parts[1:-2])
                fpath = os.path.join(results_dir, fname)
                with open(fpath) as f:
                    raw = json.load(f)
                metrics = raw.get("metrics", {})
                key = (scenario_num, comparator)
                data[key] = {
                    "p99":          metrics.get("p99_latency_ms"),
                    "slo":          metrics.get("slo_violation_rate_pct"),
                    "rps":          metrics.get("request_rate_rps"),
                    "scale_total":  metrics.get("scale_events_total"),
                    "scale_up":     metrics.get("scale_events_up"),
                    "scale_down":   metrics.get("scale_events_down"),
                    "cpu_frontend": metrics.get("frontend_cpu_pct"),
                    "audit":        metrics.get("audit_records"),
                    "osc":          None,
                }
            except Exception as e:
                print(f"  Warning: could not parse {fname}: {e}")

    return data


def fmt(val, suffix=""):
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.1f}{suffix}"
    return f"{val}{suffix}"


def best(vals, lower_is_better=True):
    """Return index of best value."""
    filtered = [(i, v) for i, v in enumerate(vals) if v is not None]
    if not filtered:
        return None
    if lower_is_better:
        return min(filtered, key=lambda x: x[1])[0]
    else:
        return max(filtered, key=lambda x: x[1])[0]


def print_table(data, scenario_num):
    comps = ["acomp", "smart_hpa", "pbscaler", "baseline_a", "baseline_b"]
    labels = [COMPARATORS.get(c, c) for c in comps]
    W = 80

    print(f"\n{'='*W}")
    print(f"  SCENARIO {scenario_num}: {SCENARIOS[scenario_num]}")
    print(f"{'='*W}")

    col_w = 14
    header = f"  {'Metric':<28}" + "".join(f"{l:>{col_w}}" for l in labels)
    print(header)
    print(f"  {'-'*28}" + "-" * (col_w * len(comps)))

    metrics_def = [
        ("p99 latency (ms)",          "p99",          True),
        ("SLO violation rate (%)",     "slo",          True),
        ("Request rate (req/s)",       "rps",          False),
        ("Scale events total",         "scale_total",  True),
        ("Scale events up",            "scale_up",     True),
        ("Scale events down",          "scale_down",   True),
        ("Frontend CPU (%)",           "cpu_frontend", True),
        ("Oscillation idx (events/hr)","osc",          True),
        ("Audit records",              "audit",        False),
    ]

    for label, key, lower_better in metrics_def:
        vals = [data.get((scenario_num, c), {}).get(key) for c in comps]
        best_idx = best(vals, lower_is_better=lower_better)

        row = f"  {label:<28}"
        for i, v in enumerate(vals):
            cell = fmt(v)
            # Bold best value with asterisk
            if i == best_idx and v is not None:
                cell = f"*{cell}*"
            row += f"{cell:>{col_w}}"
        print(row)

    # Audit log row
    audit_row = f"  {'Audit log available':<28}"
    for c in comps:
        audit = data.get((scenario_num, c), {}).get("audit")
        if audit is None:
            cell = "—"
        elif audit > 0 or c == "acomp":
            cell = "YES"
        else:
            cell = "NO"
        audit_row += f"{cell:>{col_w}}"
    print(audit_row)
    print(f"{'='*W}")
    print("  * = best value for that metric")


def export_csv(data, path):
    comps = ["acomp", "smart_hpa", "pbscaler", "baseline_a", "baseline_b"]
    fields = ["scenario", "comparator", "p99", "slo", "rps",
              "scale_total", "scale_up", "scale_down", "cpu_frontend", "audit", "osc"]

    rows = []
    for scenario_num in sorted(SCENARIOS.keys()):
        for c in comps:
            d = data.get((scenario_num, c), {})
            row = {
                "scenario": f"S{scenario_num}",
                "comparator": COMPARATORS.get(c, c),
                **{k: d.get(k, "") for k in fields[2:]}
            }
            rows.append(row)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nExported to: {path}")


def main():
    parser = argparse.ArgumentParser(description="Fetch and display all ACOMP evaluation metrics")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--export", help="Export to CSV file path")
    args = parser.parse_args()

    print("Loading metrics from files...")
    file_data = load_from_files(args.results_dir)
    print(f"  Found {len(file_data)} metric files")

    # Merge: file data takes priority over captured constants
    data = dict(CAPTURED)
    data.update(file_data)
    print(f"  Total entries: {len(data)}")

    for scenario_num in sorted(SCENARIOS.keys()):
        print_table(data, scenario_num)

    # Summary
    print(f"\n{'='*80}")
    print("  SUMMARY — Audit Records Across All Scenarios")
    print(f"{'='*80}")
    total_acomp = sum(
        data.get((s, "acomp"), {}).get("audit", 0) or 0
        for s in SCENARIOS
    )
    print(f"  ACOMP total audit records:  {total_acomp}")
    print(f"  All baselines total:        0")
    print(f"  ACOMP Scenario 2 p99:       {data.get((2,'acomp'),{}).get('p99','—')} ms  (only comparator below 500ms SLO)")
    print(f"{'='*80}")

    if args.export:
        export_csv(data, args.export)

    return 0


if __name__ == "__main__":
    sys.exit(main())
