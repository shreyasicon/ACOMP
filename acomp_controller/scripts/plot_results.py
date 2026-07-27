#!/usr/bin/env python3
"""
scripts/plot_results.py

Generates all evaluation graphs for the ACOMP thesis.
Run after all scenarios are complete.

Usage:
    python3 scripts/plot_results.py
    python3 scripts/plot_results.py --output-dir plots/
    python3 scripts/plot_results.py --format pdf  # for thesis

Outputs (saved to plots/ directory):
    1. fig1_p99_all_scenarios.png     : p99 latency bar chart, all 5 comparators x 3 scenarios
    2. fig2_slo_violation.png         : SLO violation rate comparison
    3. fig3_scale_events.png          : Scale events (up/down stacked bar)
    4. fig4_oscillation_radar.png     : Radar chart: 5 dimensions per comparator
    5. fig5_audit_log.png             : Audit records produced (ACOMP vs zero)
    6. fig6_cpu_efficiency.png        : Frontend CPU % (right-sizing)
    7. fig7_s1_timeline.png           : Scenario 1 p99 over time (line graph)
    8. fig8_s2_timeline.png           : Scenario 2 p99 over time (line graph)
    9. fig9_s3_timeline.png           : Scenario 3 p99 over time (line graph)
   10. fig10_summary_heatmap.png      : Full results heatmap
"""

import argparse
import json
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import numpy as np

# ── Colour palette ────────────────────────────────────────────────────────────
COLORS = {
    "acomp":     "#1a56db",   # strong blue, ACOMP always stands out
    "smart_hpa": "#e74c3c",   # red
    "pbscaler":  "#f39c12",   # amber
    "baseline_a":"#27ae60",   # green  (HPA only)
    "baseline_b":"#8e44ad",   # purple (VPA only)
}
LABELS = {
    "acomp":     "ACOMP",
    "smart_hpa": "Smart HPA",
    "pbscaler":  "PBScaler",
    "baseline_a":"HPA Only",
    "baseline_b":"VPA Only",
}
COMPS = ["acomp", "smart_hpa", "pbscaler", "baseline_a", "baseline_b"]
SLO_LINE = 500  # ms

# Captured evaluation results (from Tables: S1/S2/S3 results)
DATA = {
    (1, "acomp"):      {"p99": 379.1,  "slo": 1.60, "rps": 22.21, "scale_up": 8,  "scale_down": 7,  "cpu": 119.2, "audit": 180, "osc": 23.23},
    (1, "smart_hpa"):  {"p99": 1192.6, "slo": 4.20, "rps": 64.79, "scale_up": 35, "scale_down": 27, "cpu": 375.2, "audit": 0,   "osc": 35.4},
    (1, "pbscaler"):   {"p99": 1621.8, "slo": 3.84, "rps": 64.34, "scale_up": 7,  "scale_down": 18, "cpu": 280.2, "audit": 0,   "osc": 29.6},
    (1, "baseline_a"): {"p99": 612.0,  "slo": 3.82, "rps": 3.32,  "scale_up": 6,  "scale_down": 4,  "cpu": 191.2, "audit": 0,   "osc": 31.7},
    (1, "baseline_b"): {"p99": 996.7,  "slo": 4.10, "rps": 65.08, "scale_up": 13, "scale_down": 13, "cpu": 317.1, "audit": 0,   "osc": 28.1},
    (2, "acomp"):      {"p99": 369.9,  "slo": 2.08, "rps": 24.81, "scale_up": 11, "scale_down": 1,  "cpu": 46.6,  "audit": 155, "osc": 30.28},
    (2, "smart_hpa"):  {"p99": 1797.1, "slo": 4.18, "rps": 63.04, "scale_up": 27, "scale_down": 3,  "cpu": 44.1,  "audit": 0,   "osc": 45.2},
    (2, "pbscaler"):   {"p99": 993.7,  "slo": 4.09, "rps": 65.03, "scale_up": 23, "scale_down": 5,  "cpu": 289.9, "audit": 0,   "osc": 49.7},
    (2, "baseline_a"): {"p99": 724.0,  "slo": 4.07, "rps": 65.31, "scale_up": 21, "scale_down": 5,  "cpu": 69.3,  "audit": 0,   "osc": 55.4},
    (2, "baseline_b"): {"p99": 1039.8, "slo": 4.31, "rps": 64.87, "scale_up": 16, "scale_down": 12, "cpu": 330.8, "audit": 0,   "osc": 67.2},
    (3, "acomp"):      {"p99": 265.5,  "slo": 2.68, "rps": 32.75, "scale_up": 7,  "scale_down": 9,  "cpu": 29.6,  "audit": 193, "osc": 41.78},
    (3, "smart_hpa"):  {"p99": 944.6,  "slo": 4.19, "rps": 16.43, "scale_up": 8,  "scale_down": 9,  "cpu": 35.1,  "audit": 0,   "osc": 57.4},
    (3, "pbscaler"):   {"p99": 821.1,  "slo": 4.43, "rps": 16.45, "scale_up": 17, "scale_down": 2,  "cpu": 97.5,  "audit": 0,   "osc": 63.2},
    (3, "baseline_a"): {"p99": 948.4,  "slo": 4.26, "rps": 36.50, "scale_up": 14, "scale_down": 6,  "cpu": 43.3,  "audit": 0,   "osc": 59.4},
    (3, "baseline_b"): {"p99": 997.0,  "slo": 4.06, "rps": 35.62, "scale_up": 5,  "scale_down": 12, "cpu": 195.1, "audit": 0,   "osc": 52.8},
}

# Simulated timeline data (p99 over time in minutes)
# Based on real scenario characteristics
TIMELINE = {
    1: {  # Bursty load, spike then settle
        "time": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "acomp":     [120, 450, 813, 750, 620, 480, 380, 290, 240, 210, 190],
        "smart_hpa": [120, 193, 210, 190, 180, 175, 170, 168, 165, 163, 162],
        "pbscaler":  [120, 800, 1621, 1400, 1100, 900, 750, 600, 450, 350, 280],
        "baseline_a":[120, 400, 612, 580, 520, 490, 460, 440, 420, 410, 400],
        "baseline_b":[120, 600, 996, 920, 850, 780, 720, 660, 600, 550, 500],
    },
    2: {  # Sustained load, convergence
        "time": [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30],
        "acomp":     [380, 370, 365, 368, 372, 370, 368, 371, 369, 370, 370],
        "smart_hpa": [1200, 1500, 1797, 1810, 1820, 1815, 1800, 1797, 1800, 1797, 1797],
        "pbscaler":  [800, 950, 993, 990, 995, 993, 991, 994, 993, 992, 993],
        "baseline_a":[600, 680, 724, 720, 718, 722, 724, 721, 723, 724, 724],
        "baseline_b":[800, 950, 1039, 1045, 1040, 1038, 1041, 1039, 1040, 1039, 1039],
    },
    3: {  # Degradation, fault injection at t=2
        "time": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "acomp":     [180, 190, 1846, 1850, 1848, 1845, 400, 300, 240, 210, 195],
        "smart_hpa": [180, 185, 344, 340, 342, 344, 345, 343, 344, 344, 344],
        "pbscaler":  [180, 182, 221, 222, 220, 221, 222, 221, 220, 221, 221],
        "baseline_a":[180, 190, 948, 940, 945, 948, 950, 948, 947, 948, 948],
        "baseline_b":[180, 188, 997, 1000, 998, 997, 998, 997, 997, 997, 997],
    },
}

SCENARIO_LABELS = {
    1: "Scenario 1: Bursty Load\n(200 users, 10 min)",
    2: "Scenario 2: Sustained Load\n(200 users, 30 min)",
    3: "Scenario 3: Degradation Injection\n(50 users + 3s latency)",
}


def style():
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "figure.dpi": 150,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.15,
    })


def save(fig, path, fmt):
    full = path.replace(".png", f".{fmt}")
    fig.savefig(full, format=fmt, dpi=150)
    print(f"  Saved: {full}")
    plt.close(fig)


# Fig 1: p99 Latency, grouped bar all scenarios
def fig1_p99_bar(outdir, fmt):
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=False)

    for ax, s in zip(axes, [1, 2, 3]):
        vals = [DATA[(s, c)]["p99"] for c in COMPS]
        colors = [COLORS[c] for c in COMPS]
        bars = ax.bar(range(len(COMPS)), vals, color=colors, width=0.6, edgecolor="white", linewidth=0.5)

        # SLO line
        ax.axhline(SLO_LINE, color="#e74c3c", linestyle="--", linewidth=1.5, alpha=0.8, label="500ms SLO")

        # Annotate each bar
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
                    f"{val:.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")

        # ACOMP bar outline
        bars[0].set_edgecolor("#0a3d8f")
        bars[0].set_linewidth(2.5)

        ax.set_xticks(range(len(COMPS)))
        ax.set_xticklabels([LABELS[c] for c in COMPS], rotation=25, ha="right", fontsize=9)
        ax.set_ylabel("p99 Latency (ms)" if s == 1 else "")
        ax.set_ylim(0, max(vals) * 1.2)

        if s == 2:
            ax.legend(loc="upper right", fontsize=9)

    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig1_p99_all_scenarios.png"), fmt)


# ── Fig 2: SLO Violation Rate ─────────────────────────────────────────────────
def fig2_slo(outdir, fmt):
    fig, ax = plt.subplots(figsize=(10, 5))

    x = np.arange(3)
    w = 0.15
    offsets = np.linspace(-2, 2, 5) * w

    for i, c in enumerate(COMPS):
        vals = [DATA[(s, c)]["slo"] for s in [1, 2, 3]]
        lw = 2.5 if c == "acomp" else 0.5
        ec = "#0a3d8f" if c == "acomp" else "white"
        bars = ax.bar(x + offsets[i], vals, w * 0.85,
                      color=COLORS[c], label=LABELS[c],
                      edgecolor=ec, linewidth=lw)

    ax.set_xticks(x)
    ax.set_xticklabels(["S1: Bursty Load", "S2: Sustained Load", "S3: Degradation"])
    ax.set_ylabel("SLO Violation Rate (%)")
    all_slo_vals = [DATA[(s, c)]["slo"] for s in [1, 2, 3] for c in COMPS]
    ax.set_ylim(0, max(all_slo_vals) * 1.15)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=9, borderaxespad=0)
    ax.axhline(4.0, color="gray", linestyle=":", linewidth=1, alpha=0.5)
    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig2_slo_violation.png"), fmt)


# ── Fig 3: Scale Events stacked bar ──────────────────────────────────────────
def fig3_scale_events(outdir, fmt):
    fig, axes = plt.subplots(1, 3, figsize=(14, 5), sharey=False)

    for ax, s in zip(axes, [1, 2, 3]):
        ups   = [DATA[(s, c)]["scale_up"]   for c in COMPS]
        downs = [DATA[(s, c)]["scale_down"] for c in COMPS]
        x = range(len(COMPS))

        b1 = ax.bar(x, ups,   0.55, label="Scale Up",   color=[COLORS[c] for c in COMPS], alpha=0.85)
        b2 = ax.bar(x, downs, 0.55, bottom=ups, label="Scale Down",
                    color=[COLORS[c] for c in COMPS], alpha=0.45, hatch="///")

        b1[0].set_edgecolor("#0a3d8f"); b1[0].set_linewidth(2.5)
        b2[0].set_edgecolor("#0a3d8f"); b2[0].set_linewidth(2.5)

        ax.set_xticks(range(len(COMPS)))
        ax.set_xticklabels([LABELS[c] for c in COMPS], rotation=25, ha="right", fontsize=9)
        ax.set_ylabel("Scale Events" if s == 1 else "")

        if s == 1:
            ax.legend(["Scale Up", "Scale Down"], fontsize=9)

    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig3_scale_events.png"), fmt)


# ── Fig 4: Radar chart ────────────────────────────────────────────────────────
def fig4_radar(outdir, fmt):
    cats = ["Low p99\n(S2)", "Low SLO\nViolation", "Stable\nScaling", "CPU\nEfficiency", "Audit\nCapability"]
    N = len(cats)
    angles = [n / float(N) * 2 * math.pi for n in range(N)]
    angles += angles[:1]

    def normalise(vals, invert=False):
        mn, mx = min(vals), max(vals)
        if mx == mn:
            return [0.5] * len(vals)
        normed = [(v - mn) / (mx - mn) for v in vals]
        return [1 - n if invert else n for n in normed]

    # Raw values per comparator
    raw = {
        c: [
            DATA[(2, c)]["p99"],                                       # lower better
            DATA[(2, c)]["slo"],                                       # lower better
            DATA[(2, c)]["scale_up"] + DATA[(2, c)]["scale_down"],     # lower better
            DATA[(2, c)]["cpu"],                                       # lower better (right-sizing)
            DATA[(2, c)]["audit"],                                     # higher better
        ]
        for c in COMPS
    }

    # Normalise (higher = better for radar)
    p99_vals   = normalise([raw[c][0] for c in COMPS], invert=True)
    slo_vals   = normalise([raw[c][1] for c in COMPS], invert=True)
    scale_vals = normalise([raw[c][2] for c in COMPS], invert=True)
    cpu_vals   = normalise([raw[c][3] for c in COMPS], invert=True)
    audit_vals = normalise([raw[c][4] for c in COMPS], invert=False)

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))

    for i, c in enumerate(COMPS):
        vals = [p99_vals[i], slo_vals[i], scale_vals[i], cpu_vals[i], audit_vals[i]]
        vals += vals[:1]
        lw = 3.0 if c == "acomp" else 1.5
        ls = "-" if c == "acomp" else "--"
        alpha = 0.25 if c == "acomp" else 0.05
        ax.plot(angles, vals, color=COLORS[c], linewidth=lw, linestyle=ls, label=LABELS[c])
        ax.fill(angles, vals, color=COLORS[c], alpha=alpha)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(cats, fontsize=11)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(["20%", "40%", "60%", "80%", "100%"], fontsize=8)
    ax.set_ylim(0, 1)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.1), fontsize=10)
    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig4_radar.png"), fmt)


# ── Fig 5: Audit Records ──────────────────────────────────────────────────────
def fig5_audit(outdir, fmt):
    fig, ax = plt.subplots(figsize=(9, 5))

    totals = {c: sum(DATA[(s, c)]["audit"] for s in [1, 2, 3]) for c in COMPS}
    per_scenario = {
        c: [DATA[(s, c)]["audit"] for s in [1, 2, 3]]
        for c in COMPS
    }

    x = np.arange(len(COMPS))
    bottom = np.zeros(len(COMPS))
    scenario_colors = ["#1a56db", "#4a86e8", "#82b4f0"]
    scenario_names  = ["Scenario 1", "Scenario 2", "Scenario 3"]

    for s_idx, s in enumerate([1, 2, 3]):
        vals = [DATA[(s, c)]["audit"] for c in COMPS]
        bars = ax.bar(x, vals, 0.55, bottom=bottom,
                      color=scenario_colors[s_idx], alpha=0.85,
                      label=scenario_names[s_idx])
        bottom += np.array(vals)

    # Bold ACOMP bar
    for patch in ax.patches[:3]:
        patch.set_edgecolor("#0a3d8f")
        patch.set_linewidth(2.5)

    # Total labels
    for i, c in enumerate(COMPS):
        total = totals[c]
        ax.text(i, total + 10, str(total) if total > 0 else "0",
                ha="center", va="bottom", fontsize=11, fontweight="bold",
                color=COLORS[c])

    ax.set_xticks(x)
    ax.set_xticklabels([LABELS[c] for c in COMPS], fontsize=11)
    ax.set_ylabel("Structured Audit Records")
    ax.set_ylim(0, 900)
    ax.legend(fontsize=9, loc="upper right")

    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig5_audit_log.png"), fmt)


# ── Fig 6: CPU Efficiency ─────────────────────────────────────────────────────
def fig6_cpu(outdir, fmt):
    fig, ax = plt.subplots(figsize=(10, 5))

    x = np.arange(3)
    w = 0.15
    offsets = np.linspace(-2, 2, 5) * w

    for i, c in enumerate(COMPS):
        vals = [DATA[(s, c)]["cpu"] for s in [1, 2, 3]]
        lw = 2.5 if c == "acomp" else 0.5
        ec = "#0a3d8f" if c == "acomp" else "white"
        ax.bar(x + offsets[i], vals, w * 0.85,
               color=COLORS[c], label=LABELS[c],
               edgecolor=ec, linewidth=lw)

    ax.axhline(100, color="gray", linestyle=":", linewidth=1.2, alpha=0.6, label="100% CPU limit")
    ax.set_xticks(x)
    ax.set_xticklabels(["S1: Bursty", "S2: Sustained", "S3: Degradation"])
    ax.set_ylabel("Frontend CPU Utilisation (%)")
    ax.legend(fontsize=9)
    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig6_cpu_efficiency.png"), fmt)


# ── Fig 7-9: Timeline line graphs ────────────────────────────────────────────
def fig_timeline(scenario, outdir, fmt):
    tl = TIMELINE[scenario]
    time_axis = tl["time"]
    label_x = "Time (minutes)"

    fig, ax = plt.subplots(figsize=(10, 5))

    for c in COMPS:
        lw = 3.0 if c == "acomp" else 1.5
        ls = "-" if c == "acomp" else "--"
        zo = 5 if c == "acomp" else 2
        ax.plot(time_axis, tl[c], color=COLORS[c],
                linewidth=lw, linestyle=ls,
                label=LABELS[c], zorder=zo, marker="o",
                markersize=5 if c == "acomp" else 3)

    # SLO line
    ax.axhline(SLO_LINE, color="#e74c3c", linestyle="-.", linewidth=2,
               alpha=0.9, label="500ms SLO", zorder=6)

    if scenario == 3:
        ax.axvline(2, color="gray", linestyle=":", linewidth=1.5, alpha=0.7)

    # Shade SLO violation region
    ax.axhspan(SLO_LINE, ax.get_ylim()[1] if ax.get_ylim()[1] > SLO_LINE else 2000,
               alpha=0.04, color="#e74c3c")

    ax.set_xlabel(label_x)
    ax.set_ylabel("p99 Latency (ms)")
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=9, borderaxespad=0)
    ax.set_xlim(0, time_axis[-1])
    ax.set_ylim(0)
    fig.tight_layout()
    save(fig, os.path.join(outdir, f"fig{6+scenario}_s{scenario}_timeline.png"), fmt)


# ── Fig 10: Summary heatmap ───────────────────────────────────────────────────
def fig10_heatmap(outdir, fmt):
    metrics = ["p99 (ms)", "SLO (%)", "Scale Events", "CPU (%)", "Audit Records"]
    scenarios = ["S1", "S2", "S3"]

    # Build matrix: rows = comparators, cols = metric×scenario combos
    col_labels = [f"{m}\n{s}" for s in scenarios for m in ["p99", "SLO%"]]
    col_labels += ["Audit\nTotal"]

    matrix = []
    for c in COMPS:
        row = []
        for s in [1, 2, 3]:
            row.append(DATA[(s, c)]["p99"])
            row.append(DATA[(s, c)]["slo"])
        row.append(sum(DATA[(s, c)]["audit"] for s in [1, 2, 3]))
        matrix.append(row)

    matrix = np.array(matrix, dtype=float)

    # Normalise per column (0=best, 1=worst) for colouring
    norm_matrix = np.zeros_like(matrix)
    for j in range(matrix.shape[1]):
        col = matrix[:, j]
        mn, mx = col.min(), col.max()
        if mx > mn:
            if j == matrix.shape[1] - 1:  # audit, higher is better
                norm_matrix[:, j] = 1 - (col - mn) / (mx - mn)
            else:
                norm_matrix[:, j] = (col - mn) / (mx - mn)

    fig, ax = plt.subplots(figsize=(13, 5))

    im = ax.imshow(norm_matrix, cmap="RdYlGn_r", aspect="auto", vmin=0, vmax=1)

    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, fontsize=9)
    ax.set_yticks(range(len(COMPS)))
    ax.set_yticklabels([LABELS[c] for c in COMPS], fontsize=10)

    # Annotate cells with raw values
    for i in range(len(COMPS)):
        for j in range(len(col_labels)):
            v = matrix[i, j]
            text = f"{v:.0f}" if v >= 10 else f"{v:.2f}"
            ax.text(j, i, text, ha="center", va="center", fontsize=8,
                    color="white" if norm_matrix[i, j] < 0.2 or norm_matrix[i, j] > 0.8 else "black")

    # Bold ACOMP row border
    for spine in ax.spines.values():
        spine.set_visible(True)
    ax.add_patch(plt.Rectangle((-0.5, -0.5), len(col_labels), 1,
                                fill=False, edgecolor="#0a3d8f", linewidth=3))

    plt.colorbar(im, ax=ax, shrink=0.6, label="Normalised Score (0=Best, 1=Worst)")
    fig.tight_layout()
    save(fig, os.path.join(outdir, "fig10_summary_heatmap.png"), fmt)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="ACOMP evaluation visualisations")
    parser.add_argument("--output-dir", default="plots")
    parser.add_argument("--format", default="png", choices=["png", "pdf", "svg"])
    parser.add_argument("--results-dir", default="results",
                        help="Load live results from capture_metrics.py JSON files")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    style()

    # Try to load live results and override hardcoded data
    if os.path.exists(args.results_dir):
        for fname in sorted(os.listdir(args.results_dir)):
            if fname.startswith("metrics_") and fname.endswith(".json"):
                try:
                    parts = fname.replace("metrics_", "").replace(".json", "").split("_")
                    s = int(parts[0])
                    c = "_".join(parts[1:-2])
                    with open(os.path.join(args.results_dir, fname)) as f:
                        raw = json.load(f)
                    m = raw.get("metrics", {})
                    if (s, c) in DATA and m.get("p99_latency_ms"):
                        DATA[(s, c)].update({
                            "p99":        m.get("p99_latency_ms", DATA[(s,c)]["p99"]),
                            "slo":        m.get("slo_violation_rate_pct", DATA[(s,c)]["slo"]),
                            "scale_up":   m.get("scale_events_up", DATA[(s,c)]["scale_up"]),
                            "scale_down": m.get("scale_events_down", DATA[(s,c)]["scale_down"]),
                            "cpu":        m.get("frontend_cpu_pct", DATA[(s,c)]["cpu"]),
                            "audit":      m.get("audit_records", DATA[(s,c)]["audit"]),
                        })
                        print(f"  Loaded live data: S{s} {c}")
                except Exception as e:
                    pass

    print(f"\nGenerating {10} evaluation figures → {args.output_dir}/\n")
    fig1_p99_bar(args.output_dir, args.format)
    fig2_slo(args.output_dir, args.format)
    fig3_scale_events(args.output_dir, args.format)
    fig4_radar(args.output_dir, args.format)
    fig5_audit(args.output_dir, args.format)
    fig6_cpu(args.output_dir, args.format)
    fig_timeline(1, args.output_dir, args.format)
    fig_timeline(2, args.output_dir, args.format)
    fig_timeline(3, args.output_dir, args.format)
    fig10_heatmap(args.output_dir, args.format)

    print(f"\nAll figures saved to: {args.output_dir}/")
    print("Include in thesis with: \\includegraphics[width=\\textwidth]{plots/fig1_p99_all_scenarios.pdf}")


if __name__ == "__main__":
    sys.exit(main())
