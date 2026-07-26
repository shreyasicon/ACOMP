#!/usr/bin/env python3
"""
scripts/suggest_event_scaling.py

Analyses upcoming events in alomp_config.yaml and suggests
pre-scaled replica counts with cost estimates.

Usage:
    python3 scripts/suggest_event_scaling.py
    python3 scripts/suggest_event_scaling.py --event "Black Friday 2026"
    python3 scripts/suggest_event_scaling.py --list-events
    python3 scripts/suggest_event_scaling.py --activate "Black Friday 2026"
    python3 scripts/suggest_event_scaling.py --deactivate "Black Friday 2026"
    python3 scripts/suggest_event_scaling.py --mode aggressive
    python3 scripts/suggest_event_scaling.py --budget-status
"""

import argparse
import json
import os
import subprocess
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from acomp.adaptive_engine import AdaptiveEngine, BUILTIN_MODES


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "alomp_config.yaml"
)


def get_current_replicas() -> dict[str, int]:
    """Get live replica counts from cluster."""
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
                 "-o", "jsonpath={.spec.replicas}"],
                stderr=subprocess.DEVNULL
            ).decode().strip()
            replicas[svc] = int(result) if result else 1
        except Exception:
            replicas[svc] = 1
    return replicas


def set_mode(mode_name: str):
    """Hot-swap ACOMP operating mode in config file."""
    if mode_name not in BUILTIN_MODES:
        print(f"Unknown mode: {mode_name}. Choose: strategic, aggressive, conservative")
        return

    with open(CONFIG_PATH) as f:
        content = f.read()

    import re
    new_content = re.sub(r'^mode: \w+', f'mode: {mode_name}', content, flags=re.MULTILINE)

    with open(CONFIG_PATH, "w") as f:
        f.write(new_content)

    print(f"\n✓ Mode changed to: {mode_name}")
    print(f"  ACOMP will hot-reload within 5 seconds — no restart needed")
    print(f"\n  {BUILTIN_MODES[mode_name].description}")


def set_event_status(event_name: str, status: str):
    """Activate or deactivate an event in config file."""
    with open(CONFIG_PATH) as f:
        raw = yaml.safe_load(f)

    found = False
    for event in raw.get("events", []):
        if event["name"] == event_name:
            event["status"] = status
            found = True
            break

    if not found:
        print(f"Event not found: {event_name}")
        return

    with open(CONFIG_PATH, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\n✓ Event '{event_name}' set to: {status}")
    print(f"  ACOMP will hot-reload within 5 seconds")


def print_suggestion(suggestion: dict):
    W = 65
    print(f"\n{'='*W}")
    print(f"  Event Scaling Suggestion: {suggestion['event']}")
    print(f"{'='*W}")
    print(f"  Expected users:      {suggestion['expected_users']}")
    print(f"  Load multiplier:     {suggestion['load_multiplier']}")
    print(f"  Mode override:       {suggestion['mode_override'].upper()}")
    print(f"  Pre-scale window:    {suggestion['pre_scale_hours_before']}h before event")
    if suggestion.get("notes"):
        print(f"  Notes:               {suggestion['notes']}")
    print(f"\n  {'Service':<30} {'Current':>8} {'Suggested':>10} {'Delta':>6}")
    print(f"  {'-'*30} {'-'*8} {'-'*10} {'-'*6}")
    for svc, info in suggestion["suggestions"].items():
        delta_str = f"+{info['delta']}" if info['delta'] >= 0 else str(info['delta'])
        marker = " ▲" if info['delta'] > 0 else "  "
        print(f"  {svc:<30} {info['current']:>8} {info['suggested']:>10} {delta_str:>6}{marker}")
    print(f"\n  Total additional replicas: {suggestion['total_additional_replicas']}")
    print(f"  Estimated cost delta:      {suggestion['estimated_cost_delta_per_hr']}/hr")
    print(f"{'='*W}")


def main():
    parser = argparse.ArgumentParser(description="ACOMP Event Scaling Advisor")
    parser.add_argument("--event", help="Show suggestion for specific event")
    parser.add_argument("--list-events", action="store_true", help="List all events")
    parser.add_argument("--activate", metavar="EVENT", help="Activate an event")
    parser.add_argument("--deactivate", metavar="EVENT", help="Deactivate an event")
    parser.add_argument("--mode", choices=["strategic", "aggressive", "conservative"],
                        help="Switch ACOMP operating mode")
    parser.add_argument("--budget-status", action="store_true",
                        help="Show anomaly budget status per service")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to alomp_config.yaml")
    args = parser.parse_args()

    engine = AdaptiveEngine(args.config)

    if args.mode:
        set_mode(args.mode)
        return

    if args.activate:
        set_event_status(args.activate, "active")
        return

    if args.deactivate:
        set_event_status(args.deactivate, "inactive")
        return

    if args.budget_status:
        summary = engine.budget_summary()
        print(f"\n{'='*65}")
        print(f"  Anomaly Budget Status — {__import__('datetime').date.today()}")
        print(f"{'='*65}")
        print(f"  {'Service':<30} {'Used':>6} {'Budget':>8} {'Remaining':>10} {'Status':>12}")
        print(f"  {'-'*30} {'-'*6} {'-'*8} {'-'*10} {'-'*12}")
        for svc, info in summary.items():
            status = "⚠ EXHAUSTED" if info["exhausted"] else f"{info['pct_used']:.0f}% used"
            print(f"  {svc:<30} {info['violations']:>6} {info['budget']:>8} "
                  f"{info['remaining']:>10} {status:>12}")
        print(f"{'='*65}\n")
        return

    if args.list_events:
        with open(args.config) as f:
            raw = yaml.safe_load(f)
        events = raw.get("events", [])
        print(f"\n{'='*65}")
        print(f"  Configured Events")
        print(f"{'='*65}")
        for e in events:
            status_icon = "✓" if e["status"] == "active" else "○"
            print(f"  {status_icon} [{e['status'].upper():<8}] {e['name']}")
            print(f"    Start: {e['start']}  End: {e['end']}")
            print(f"    Expected: {e['expected_users']} users  Mode: {e.get('mode_override','strategic')}")
            if e.get("notes"):
                print(f"    Notes: {e['notes']}")
            print()
        return

    # Default — show suggestions for all active or upcoming events
    with open(args.config) as f:
        raw = yaml.safe_load(f)

    events = raw.get("events", [])
    target_events = [e for e in events if e["status"] in ("active", "inactive")]

    if args.event:
        target_events = [e for e in target_events if e["name"] == args.event]

    if not target_events:
        print("No events found. Use --list-events to see all configured events.")
        return

    print(f"\nFetching current replica counts from cluster...")
    try:
        current_replicas = get_current_replicas()
        print(f"  Got replica counts for {len(current_replicas)} services")
    except Exception as e:
        print(f"  Could not connect to cluster ({e}) — using baseline of 1 replica")
        current_replicas = {svc: 1 for svc in [
            "frontend", "currencyservice", "productcatalogservice",
            "cartservice", "recommendationservice", "checkoutservice",
            "paymentservice", "shippingservice", "emailservice", "adservice"
        ]}

    for e in target_events:
        suggestion = engine.suggest_event_scaling(e["name"], current_replicas)
        if "error" not in suggestion:
            print_suggestion(suggestion)

    print(f"\nTo activate an event:")
    print(f"  python3 scripts/suggest_event_scaling.py --activate \"Event Name\"")
    print(f"\nTo switch mode:")
    print(f"  python3 scripts/suggest_event_scaling.py --mode aggressive")
    print(f"  python3 scripts/suggest_event_scaling.py --mode conservative")
    print(f"  python3 scripts/suggest_event_scaling.py --mode strategic")


if __name__ == "__main__":
    main()
