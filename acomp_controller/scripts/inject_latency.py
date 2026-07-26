#!/usr/bin/env python3
"""
scripts/inject_latency.py

Injects network latency into a Kubernetes pod using tc-netem.
This works at the kernel network level — unlike EXTRA_LATENCY_MILLIS
which relies on the application binary reading an env var.

Usage:
    # Inject 3000ms latency into recommendationservice
    python3 scripts/inject_latency.py --service recommendationservice --latency 3000

    # Remove injected latency
    python3 scripts/inject_latency.py --service recommendationservice --remove

    # Check current tc rules
    python3 scripts/inject_latency.py --service recommendationservice --check

How it works:
    Uses 'kubectl exec' to run 'tc qdisc add dev eth0 root netem delay Xms'
    inside the pod. This adds a kernel-level network delay on all outbound
    traffic from the pod, making it appear slow to upstream callers.
    Works regardless of what the application binary does.
"""

import argparse
import subprocess
import sys
import time


def get_pod_name(service: str, namespace: str = "default") -> str | None:
    """Get the first running pod name for a deployment."""
    try:
        result = subprocess.check_output([
            "kubectl", "get", "pods",
            "-n", namespace,
            "-l", f"app={service}",
            "--field-selector=status.phase=Running",
            "-o", "jsonpath={.items[0].metadata.name}"
        ], stderr=subprocess.DEVNULL).decode().strip()
        return result if result else None
    except Exception as e:
        print(f"Error getting pod for {service}: {e}")
        return None


def check_tc_rules(pod: str, namespace: str = "default") -> str:
    """Check current tc rules in a pod."""
    try:
        result = subprocess.check_output([
            "kubectl", "exec", "-n", namespace, pod,
            "--", "tc", "qdisc", "show", "dev", "eth0"
        ], stderr=subprocess.STDOUT).decode().strip()
        return result
    except Exception as e:
        return f"Error: {e}"


def install_tc_if_needed(pod: str, namespace: str = "default") -> bool:
    """Install iproute2 (tc) if not present in pod."""
    try:
        subprocess.check_output([
            "kubectl", "exec", "-n", namespace, pod,
            "--", "tc", "--version"
        ], stderr=subprocess.DEVNULL)
        return True
    except Exception:
        print(f"  tc not found in {pod}, attempting to install iproute2...")
        try:
            subprocess.check_output([
                "kubectl", "exec", "-n", namespace, pod,
                "--", "sh", "-c",
                "apt-get update -qq && apt-get install -y -qq iproute2 2>/dev/null || "
                "apk add --quiet iproute2 2>/dev/null || "
                "yum install -y -q iproute 2>/dev/null"
            ], stderr=subprocess.STDOUT, timeout=60)
            return True
        except Exception as e:
            print(f"  Could not install iproute2: {e}")
            return False


def inject_latency(pod: str, latency_ms: int, namespace: str = "default") -> bool:
    """Inject network latency using tc-netem."""
    # First remove any existing rules
    try:
        subprocess.run([
            "kubectl", "exec", "-n", namespace, pod,
            "--", "tc", "qdisc", "del", "dev", "eth0", "root"
        ], capture_output=True)
    except Exception:
        pass

    # Add netem delay
    try:
        subprocess.check_output([
            "kubectl", "exec", "-n", namespace, pod,
            "--", "tc", "qdisc", "add", "dev", "eth0",
            "root", "netem", "delay", f"{latency_ms}ms"
        ], stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error injecting latency: {e.output.decode()}")
        return False


def remove_latency(pod: str, namespace: str = "default") -> bool:
    """Remove tc-netem rules."""
    try:
        subprocess.check_output([
            "kubectl", "exec", "-n", namespace, pod,
            "--", "tc", "qdisc", "del", "dev", "eth0", "root"
        ], stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError as e:
        output = e.output.decode()
        if "No such file" in output or "RTNETLINK" in output:
            return True  # Already clean
        print(f"  Error removing latency: {output}")
        return False


def main():
    parser = argparse.ArgumentParser(description="tc-netem latency injection for Scenario 3")
    parser.add_argument("--service", default="recommendationservice",
                        help="Kubernetes deployment/service name")
    parser.add_argument("--latency", type=int, default=3000,
                        help="Latency to inject in milliseconds (default: 3000)")
    parser.add_argument("--duration", type=int, default=300,
                        help="How long to hold injection in seconds (default: 300)")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--remove", action="store_true",
                        help="Remove injected latency")
    parser.add_argument("--check", action="store_true",
                        help="Check current tc rules")
    parser.add_argument("--all-pods", action="store_true",
                        help="Inject into ALL pods of the service (more thorough)")
    args = parser.parse_args()

    print(f"\nACOMP Latency Injection — {args.service}")
    print(f"{'='*50}")

    # Get all pods for the service
    try:
        all_pods = subprocess.check_output([
            "kubectl", "get", "pods",
            "-n", args.namespace,
            "-l", f"app={args.service}",
            "--field-selector=status.phase=Running",
            "-o", "jsonpath={.items[*].metadata.name}"
        ], stderr=subprocess.DEVNULL).decode().strip().split()
    except Exception as e:
        print(f"Error getting pods: {e}")
        return 1

    if not all_pods:
        print(f"No running pods found for service: {args.service}")
        return 1

    pods = all_pods if args.all_pods else [all_pods[0]]
    print(f"Target pods: {', '.join(pods)}")

    # Check mode
    if args.check:
        for pod in pods:
            print(f"\nTC rules in {pod}:")
            print(f"  {check_tc_rules(pod, args.namespace)}")
        return 0

    # Remove mode
    if args.remove:
        for pod in pods:
            print(f"Removing latency from {pod}...")
            if remove_latency(pod, args.namespace):
                print(f"  ✓ Removed")
            else:
                print(f"  ✗ Failed")
        return 0

    # Inject mode
    print(f"Injecting {args.latency}ms latency into {args.service}...")
    for pod in pods:
        print(f"\n  Pod: {pod}")
        if not install_tc_if_needed(pod, args.namespace):
            print(f"  ✗ Cannot install tc — skipping {pod}")
            continue
        if inject_latency(pod, args.latency, args.namespace):
            rules = check_tc_rules(pod, args.namespace)
            print(f"  ✓ Injected — tc rules: {rules}")
        else:
            print(f"  ✗ Injection failed for {pod}")

    print(f"\n{'='*50}")
    print(f"Holding latency for {args.duration} seconds...")
    print("(Press Ctrl+C to remove early)")

    try:
        for remaining in range(args.duration, 0, -10):
            print(f"  {remaining}s remaining...", end="\r")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nInterrupted — removing latency...")

    print(f"\nRemoving injected latency...")
    for pod in pods:
        if remove_latency(pod, args.namespace):
            print(f"  ✓ {pod} cleaned")
        else:
            print(f"  ✗ {pod} cleanup failed — run with --remove manually")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
