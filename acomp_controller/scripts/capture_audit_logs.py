#!/usr/bin/env python3
"""
scripts/capture_audit_logs.py

Captures ACOMP audit logs in real-time during a scenario run.
Run this in a second terminal alongside run_scenario.py.

Usage:
    # Terminal 1
    python3 scripts/run_scenario.py --scenario 1 --comparator acomp

    # Terminal 2
    python3 scripts/capture_audit_logs.py --scenario 1

    # Or run it in background automatically
    python3 scripts/capture_audit_logs.py --scenario 1 --background &
    python3 scripts/run_scenario.py --scenario 1 --comparator acomp
    wait

Outputs:
    results/audit_logs/scenario_1_acomp_TIMESTAMP.jsonl   raw JSON Lines
    results/audit_logs/scenario_1_acomp_TIMESTAMP.txt     human readable summary
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone


def get_pod_name(namespace: str = "default") -> str | None:
    """Get the running ACOMP controller pod name."""
    try:
        result = subprocess.check_output([
            "kubectl", "get", "pods",
            "-n", namespace,
            "-l", "app=acomp-controller",
            "--field-selector=status.phase=Running",
            "-o", "jsonpath={.items[0].metadata.name}"
        ], stderr=subprocess.DEVNULL).decode().strip()
        return result if result else None
    except Exception:
        return None


def stream_logs(pod: str, namespace: str, since_seconds: int = 5):
    """Stream kubectl logs from the ACOMP controller pod."""
    return subprocess.Popen(
        [
            "kubectl", "logs", "-f",
            "-n", namespace,
            pod,
            f"--since={since_seconds}s",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )


def parse_audit_line(line: str) -> dict | None:
    """Extract JSON audit record from a log line if present."""
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        record = json.loads(line)
        # Must have pipeline_state to be an audit record
        if "pipeline_state" not in record:
            return None
        return record
    except json.JSONDecodeError:
        return None


def format_record(record: dict, idx: int) -> str:
    """Format a single audit record as human-readable text."""
    ts = record.get("timestamp", "")[:19].replace("T", " ")
    state = record.get("pipeline_state", "UNKNOWN")
    root = record.get("root_cause_service", "none")
    cycle = record.get("cycle_number", idx)
    actuation = record.get("actuation_summary", {})
    applied = actuation.get("applied", 0)
    skipped = actuation.get("skipped", 0)
    reasoning = record.get("reasoning", "")[:120]

    # Scale decisions
    decisions = record.get("decisions", [])
    scaled = [f"{d['service']}({d.get('current_replicas',0)}→{d.get('target_replicas',0)})"
              for d in decisions if d.get("outcome") == "SCALE_UP"]
    suppressed_count = sum(1 for d in decisions if d.get("outcome") == "SUPPRESSED")

    lines = [
        f"[{cycle:04d}] {ts} | {state:<30} | root={root}",
        f"       applied={applied} skipped={skipped} suppressed={suppressed_count}",
    ]
    if scaled:
        lines.append(f"       scaled: {', '.join(scaled)}")
    if reasoning:
        lines.append(f"       reason: {reasoning}...")
    lines.append("")
    return "\n".join(lines)


def git_push(results_dir: str):
    """Stage audit logs and push to git."""
    try:
        git_root = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=results_dir,
            stderr=subprocess.DEVNULL
        ).decode().strip()

        subprocess.run(["git", "add", results_dir],
                       cwd=git_root, capture_output=True)

        status = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=git_root, capture_output=True
        )
        if status.returncode == 0:
            print("  [git] Nothing new to commit")
            return

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        msg = f"Auto: audit logs captured {ts}"
        result = subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=git_root, capture_output=True, text=True
        )
        if result.returncode == 0:
            push = subprocess.run(
                ["git", "push", "origin", "main"],
                cwd=git_root, capture_output=True, text=True
            )
            if push.returncode == 0:
                print(f"  [git] Pushed audit logs: {msg}")
            else:
                print(f"  [git] Commit done, push failed — run manually")
        else:
            print(f"  [git] Commit failed: {result.stderr[:100]}")
    except Exception as e:
        print(f"  [git] Skipped: {e}")


def main():
    parser = argparse.ArgumentParser(description="ACOMP Audit Log Capture")
    parser.add_argument("--scenario", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--duration", type=int, default=0,
                        help="Stop after N seconds (0=run until Ctrl+C)")
    parser.add_argument("--background", action="store_true",
                        help="Suppress per-record console output")
    args = parser.parse_args()

    # Setup output paths
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir    = os.path.dirname(scripts_dir)
    audit_dir   = os.path.join(base_dir, "results", "audit_logs")
    os.makedirs(audit_dir, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    jsonl_path = os.path.join(audit_dir, f"scenario_{args.scenario}_acomp_{ts}.jsonl")
    txt_path   = os.path.join(audit_dir, f"scenario_{args.scenario}_acomp_{ts}.txt")

    print(f"\n{'='*60}")
    print(f"  ACOMP Audit Log Capture")
    print(f"  Scenario: {args.scenario}")
    print(f"  Output:   {jsonl_path}")
    print(f"{'='*60}")

    # Wait for ACOMP pod
    print("  Waiting for ACOMP controller pod...")
    pod = None
    for _ in range(30):
        pod = get_pod_name(args.namespace)
        if pod:
            break
        time.sleep(2)

    if not pod:
        print("  ERROR: No ACOMP controller pod found. Is it running?")
        return 1

    print(f"  Pod: {pod}")
    print(f"  Streaming logs... (Ctrl+C to stop)\n")

    # Open output files
    jsonl_fh = open(jsonl_path, "w")
    txt_fh   = open(txt_path, "w")
    txt_fh.write(f"ACOMP Audit Log — Scenario {args.scenario}\n")
    txt_fh.write(f"Captured: {ts}\n")
    txt_fh.write(f"Pod: {pod}\n")
    txt_fh.write("=" * 60 + "\n\n")

    # Stream and parse
    proc = stream_logs(pod, args.namespace)
    record_count = 0
    state_counts: dict[str, int] = {}
    start_time = time.monotonic()

    try:
        for line in proc.stdout:
            # Duration check
            if args.duration > 0:
                if time.monotonic() - start_time > args.duration:
                    print(f"\n  Duration limit reached ({args.duration}s)")
                    break

            record = parse_audit_line(line)
            if record is None:
                continue

            record_count += 1
            state = record.get("pipeline_state", "UNKNOWN")
            state_counts[state] = state_counts.get(state, 0) + 1

            # Write raw JSON Lines
            jsonl_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            jsonl_fh.flush()

            # Write human readable
            formatted = format_record(record, record_count)
            txt_fh.write(formatted)
            txt_fh.flush()

            # Console output
            if not args.background:
                actuation = record.get("actuation_summary", {})
                applied = actuation.get("applied", 0)
                marker = "⚡" if applied > 0 else "·"
                print(f"  {marker} [{record_count:04d}] {state:<30} applied={applied}")

    except KeyboardInterrupt:
        print(f"\n  Stopped by user")
    finally:
        proc.terminate()
        jsonl_fh.close()

        # Write summary to txt
        txt_fh.write("\n" + "=" * 60 + "\n")
        txt_fh.write("SUMMARY\n")
        txt_fh.write("=" * 60 + "\n")
        txt_fh.write(f"Total audit records: {record_count}\n")
        txt_fh.write(f"Duration: {time.monotonic() - start_time:.0f}s\n\n")
        txt_fh.write("Pipeline state distribution:\n")
        for state, count in sorted(state_counts.items()):
            pct = count / record_count * 100 if record_count > 0 else 0
            txt_fh.write(f"  {state:<35} {count:>5} ({pct:.1f}%)\n")
        txt_fh.close()

    # Print summary
    print(f"\n{'='*60}")
    print(f"  Captured {record_count} audit records")
    print(f"  Pipeline state distribution:")
    for state, count in sorted(state_counts.items()):
        pct = count / record_count * 100 if record_count > 0 else 0
        print(f"    {state:<35} {count:>5} ({pct:.1f}%)")
    print(f"\n  Saved:")
    print(f"    {jsonl_path}")
    print(f"    {txt_path}")
    print(f"{'='*60}\n")

    # Push to git
    print("  Pushing to git...")
    git_push(audit_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
