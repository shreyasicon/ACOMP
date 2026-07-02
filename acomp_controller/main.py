"""
main.py

ACOMP controller entry point. Wires the five components (Context Map,
Collector, Policy Engine, Actuator, Decision Logger) into a single
15-second control loop and runs it until interrupted.

This module is the only entry point: it reads configuration from environment
variables and alomp_config.yaml, initialises each component, then runs the
control loop indefinitely. It is designed to run as a Kubernetes Deployment
pod with a single replica.

Environment variables (all optional, defaults shown):
    ACOMP_CONFIG_PATH       path to alomp_config.yaml  (default: /config/alomp_config.yaml)
    ACOMP_PROMETHEUS_URL    Prometheus HTTP endpoint    (default: http://prometheus-kube-prometheus-prometheus.monitoring:9090)
    ACOMP_NAMESPACE         Kubernetes namespace        (default: default)
    ACOMP_ENTRY_POINT       entry-point service name    (default: frontend)
    ACOMP_POLL_INTERVAL     control cycle seconds       (default: 15)
    ACOMP_DRY_RUN           if "true", no K8s patches   (default: false)
    ACOMP_LOG_FILE          write logs to file instead of stdout (default: unset = stdout)
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from acomp.context_map import load_context_map
from acomp.collector import Collector
from acomp.policy_engine import PolicyEngine
from acomp.actuator import Actuator
from acomp.decision_logger import DecisionLogger

# ------------------------------------------------------------------
# Logging setup -- structured enough for Azure Monitor to parse
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stderr,   # operational logs go to stderr; decision records go to stdout
)
logger = logging.getLogger("acomp.main")


def read_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def main() -> int:
    logger.info("ACOMP controller starting")

    # ------------------------------------------------------------------
    # Configuration from environment
    # ------------------------------------------------------------------
    config_path      = read_env("ACOMP_CONFIG_PATH",    "/config/alomp_config.yaml")
    prometheus_url   = read_env("ACOMP_PROMETHEUS_URL", "http://prometheus-kube-prometheus-prometheus.monitoring:9090")
    namespace        = read_env("ACOMP_NAMESPACE",      "default")
    entry_point      = read_env("ACOMP_ENTRY_POINT",    "frontend")
    poll_interval    = float(read_env("ACOMP_POLL_INTERVAL", "15"))
    dry_run          = read_env("ACOMP_DRY_RUN", "false").lower() == "true"
    log_file         = read_env("ACOMP_LOG_FILE", "") or None

    logger.info(
        "Config: path=%s prometheus=%s namespace=%s entry_point=%s "
        "interval=%.0fs dry_run=%s",
        config_path, prometheus_url, namespace, entry_point,
        poll_interval, dry_run,
    )

    # ------------------------------------------------------------------
    # Component initialisation
    # ------------------------------------------------------------------
    logger.info("Loading Context Map from %s", config_path)
    try:
        context_map = load_context_map(config_path)
    except FileNotFoundError as exc:
        logger.error("Context Map file not found: %s", exc)
        return 1

    service_names = context_map.service_names()
    logger.info("Context Map loaded: %d services, max_replicas=%d",
                len(service_names), context_map.guardrails.max_replicas)

    collector = Collector(
        prometheus_url=prometheus_url,
        namespace=namespace,
        service_names=service_names,
        entry_point_service=entry_point,
    )

    policy_engine = PolicyEngine(context_map=context_map)

    actuator = Actuator(
        namespace=namespace,
        context_map=context_map,
        dry_run=dry_run,
    )

    decision_logger = DecisionLogger(output_file=log_file)

    # ------------------------------------------------------------------
    # Graceful shutdown on SIGTERM / SIGINT (Kubernetes sends SIGTERM
    # when a pod is being terminated)
    # ------------------------------------------------------------------
    running = True

    def _shutdown(signum, frame):
        nonlocal running
        logger.info("Received signal %d -- shutting down after current cycle", signum)
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # ------------------------------------------------------------------
    # Main control loop
    # ------------------------------------------------------------------
    cycle_number = 0
    logger.info("Control loop starting, poll interval=%.0fs", poll_interval)

    while running:
        cycle_start = time.monotonic()
        cycle_number += 1

        try:
            # Stage 1: Collect
            snapshot = collector.poll()

            # Stage 2: Decide
            decision_set, audit_record = policy_engine.run_cycle(snapshot)

            # Stage 3: Actuate
            actuator_report = actuator.apply(decision_set)

            # Stage 4: Log
            decision_logger.log(
                audit=audit_record,
                actuation=actuator_report,
                cycle_number=cycle_number,
            )

        except Exception:
            logger.exception("Unhandled error in cycle %d -- skipping", cycle_number)

        # Drift-corrected sleep: subtract time already spent this cycle
        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, poll_interval - elapsed)

        if elapsed > poll_interval:
            logger.warning(
                "Cycle %d took %.1fs > poll interval %.0fs",
                cycle_number, elapsed, poll_interval,
            )

        if running:
            time.sleep(sleep_for)

    # ------------------------------------------------------------------
    # Clean shutdown
    # ------------------------------------------------------------------
    decision_logger.close()
    logger.info("ACOMP controller stopped after %d cycles", cycle_number)
    return 0


if __name__ == "__main__":
    sys.exit(main())
