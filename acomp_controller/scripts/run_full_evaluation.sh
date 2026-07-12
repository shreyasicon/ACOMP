#!/bin/bash
# scripts/run_full_evaluation.sh
#
# Runs all three thesis evaluation scenarios for all three comparators:
#   - ACOMP (controller active, HPA disabled)
#   - Baseline A (standard Kubernetes HPA only)
#   - Baseline B (uncoordinated HPA + VPA)
#
# Total runtime: approximately 2-3 hours.
# Results saved to timestamped directories under results/
#
# Usage: bash scripts/run_full_evaluation.sh
# Run from the acomp_controller directory.

set -e

echo "======================================"
echo "  ACOMP Full Evaluation Runner"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "======================================"
echo ""

mkdir -p results

SCENARIOS="1 2 3"

# ─── ACOMP Runs ───
echo ">>> [1/3] Running all scenarios with ACOMP..."
for s in $SCENARIOS; do
  echo "--- Scenario $s (ACOMP) ---"
  python3 scripts/run_scenario.py --scenario $s --comparator acomp
  echo "Cooling down 3 minutes..."
  sleep 180
done
echo "ACOMP runs complete."
echo ""

# ─── Baseline A Runs ───
echo ">>> [2/3] Running all scenarios with Baseline A (HPA only)..."
for s in $SCENARIOS; do
  echo "--- Scenario $s (Baseline A) ---"
  python3 scripts/run_scenario.py --scenario $s --comparator baseline_a
  echo "Cooling down 3 minutes..."
  sleep 180
done
echo "Baseline A runs complete."
echo ""

# ─── Baseline B Runs ───
echo ">>> [3/3] Running all scenarios with Baseline B (HPA + VPA)..."
echo "    Note: requires VPA operator installed. Falls back to HPA-only if not."
for s in $SCENARIOS; do
  echo "--- Scenario $s (Baseline B) ---"
  python3 scripts/run_scenario.py --scenario $s --comparator baseline_b
  echo "Cooling down 3 minutes..."
  sleep 180
done
echo "Baseline B runs complete."
echo ""

# ─── Generate Comparison Tables ───
echo ">>> Generating comparison tables..."
python3 scripts/analyse_results.py

echo ""
echo "======================================"
echo "  Evaluation complete."
echo "  Results: results/"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "======================================"
