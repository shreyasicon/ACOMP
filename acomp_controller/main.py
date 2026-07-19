"""
main.py

ACOMP controller entry point — v2 with adaptive improvements.

Improvements over v1:
  1. Adaptive poll interval — polls faster under pressure (5s), slower when healthy (30s)
  2. SLO violation counter — escalates to fast polling after 3 consecutive p99 > 500ms
  3. Request rate trend detection — pre-scales when rate rising >20% per cycle
  4. Consecutive pressure tracking — logs sustained pressure streaks for audit

Environment variables (all optional, defaults shown):
    ACOMP_CONFIG_PATH           path to alomp_config.yaml  (default: /config/alomp_config.yaml)
    ACOMP_PROMETHEUS_URL        Prometheus HTTP endpoint    (default: http://prometheus-kube-prometheus-prometheus.monitoring:9090)
    ACOMP_NAMESPACE             Kubernetes namespace        (default: default)
    ACOMP_ENTRY_POINT           entry-point service name    (default: frontend)
    ACOMP_POLL_INTERVAL         base control cycle seconds  (default: 15)
    ACOMP_POLL_INTERVAL_FAST    fast cycle under pressure   (default: 5)
    ACOMP_POLL_INTERVAL_SLOW    slow cycle when healthy     (default: 30)
    ACOMP_DRY_RUN               if "true", no K8s patches   (default: false)
    ACOMP_LOG_FILE              write logs to file instead of stdout
    ACOMP_SLO_VIOLATION_WINDOW  consecutive SLO violations before escalation (default: 3)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("acomp.main")


def read_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def main() -> int:
    logger.info("ACOMP controller starting (v2 — adaptive)")

    # ── Configuration ────────────────────────────────────────────────
    config_path         = read_env("ACOMP_CONFIG_PATH",    "/config/alomp_config.yaml")
    prometheus_url      = read_env("ACOMP_PROMETHEUS_URL", "http://prometheus-kube-prometheus-prometheus.monitoring:9090")
    namespace           = read_env("ACOMP_NAMESPACE",      "default")
    entry_point         = read_env("ACOMP_ENTRY_POINT",    "frontend")
    poll_interval       = float(read_env("ACOMP_POLL_INTERVAL",      "15"))
    poll_interval_fast  = float(read_env("ACOMP_POLL_INTERVAL_FAST", "5"))
    poll_interval_slow  = float(read_env("ACOMP_POLL_INTERVAL_SLOW", "30"))
    dry_run             = read_env("ACOMP_DRY_RUN", "false").lower() == "true"
    log_file            = read_env("ACOMP_LOG_FILE", "") or None
    slo_window          = int(read_env("ACOMP_SLO_VIOLATION_WINDOW", "3"))

    logger.info(
        "Config: prometheus=%s namespace=%s entry=%s "
        "intervals: fast=%.0fs base=%.0fs slow=%.0fs slo_window=%d dry_run=%s",
        prometheus_url, namespace, entry_point,
        poll_interval_fast, poll_interval, poll_interval_slow,
        slo_window, dry_run,
    )

    # ── Component initialisation ─────────────────────────────────────
    logger.info("Loading Context Map from %s", config_path)
    try:
        context_map = load_context_map(config_path)
    except FileNotFoundError as exc:
        logger.error("Context Map file not found: %s", exc)
        return 1

    service_names = context_map.service_names()
    logger.info("Context Map loaded: %d services, max_replicas=%d",
                len(service_names), context_map.guardrails.max_replicas)

    collector      = Collector(prometheus_url=prometheus_url, namespace=namespace,
                               service_names=service_names, entry_point_service=entry_point)
    policy_engine  = PolicyEngine(context_map=context_map)
    actuator       = Actuator(namespace=namespace, context_map=context_map, dry_run=dry_run)
    decision_logger = DecisionLogger(output_file=log_file)

    # ── Graceful shutdown ────────────────────────────────────────────
    running = True

    def _shutdown(signum, frame):
        nonlocal running
        logger.info("Received signal %d -- shutting down after current cycle", signum)
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # ── Adaptive state ───────────────────────────────────────────────
    cycle_number               = 0
    consecutive_pressure       = 0   # sustained pressure streak
    consecutive_healthy        = 0   # sustained healthy streak
    consecutive_slo_violations = 0   # p99 > 500ms streak
    prev_request_rate          = None
    current_interval           = poll_interval
    last_scale_time            = 0.0  # monotonic time of last APPLIED scale action
    SCALE_COOLDOWN_SECONDS     = 30.0 # minimum seconds between scale actions

    logger.info("Control loop starting — base=%.0fs fast=%.0fs slow=%.0fs cooldown=%.0fs",
                poll_interval, poll_interval_fast, poll_interval_slow, SCALE_COOLDOWN_SECONDS)

    while running:
        cycle_start = time.monotonic()
        cycle_number += 1

        try:
            # ── Stage 1: Collect ──────────────────────────────────────
            snapshot = collector.poll()

            # ── Improvement 3: Request rate trend detection ───────────
            # If req/s rises >20% since last cycle, signal pre-scaling
            current_rate    = snapshot.request_rate
            rate_rising     = False
            if prev_request_rate and current_rate and prev_request_rate > 0:
                rate_change = (current_rate - prev_request_rate) / prev_request_rate
                if rate_change > 0.20:
                    rate_rising = True
                    logger.info(
                        "Request rate rising fast: %.2f->%.2f req/s (+%.0f%%) "
                        "-- pre-scaling signal",
                        prev_request_rate, current_rate, rate_change * 100,
                    )
            prev_request_rate = current_rate

            # ── Stage 2: Decide ───────────────────────────────────────
            decision_set, audit_record = policy_engine.run_cycle(
                snapshot, rate_rising_fast=rate_rising
            )

            # ── Stage 3: Actuate (with cooldown guard) ───────────────
            # Suppress scale actions if within cooldown window to prevent
            # rapid up/down thrashing from fast polling cycles.
            now_check = time.monotonic()
            cooldown_active = (now_check - last_scale_time) < SCALE_COOLDOWN_SECONDS
            if cooldown_active and decision_set.actionable():
                remaining = SCALE_COOLDOWN_SECONDS - (now_check - last_scale_time)
                logger.info(
                    "Cooldown active (%.0fs remaining) -- suppressing %d scale actions",
                    remaining, len(decision_set.actionable())
                )
                from acomp.actuator import ActuatorReport
                actuator_report = ActuatorReport()  # empty report, no patches sent
            else:
                actuator_report = actuator.apply(decision_set)

            # ── Stage 4: Log ──────────────────────────────────────────
            decision_logger.log(
                audit=audit_record,
                actuation=actuator_report,
                cycle_number=cycle_number,
            )

            # ── Improvement 1: Adaptive poll interval with hysteresis ────
            # Use fast polling ONLY during active pressure.
            # Require 10 consecutive HEALTHY cycles before slowing down
            # (hysteresis prevents rapid switching that caused thrashing in v2.0)
            state = audit_record.pipeline_state
            if state == "HEALTHY":
                consecutive_pressure = 0
                consecutive_healthy += 1
                # Only slow down after sustained stability (10 cycles = 150s at base)
                if consecutive_healthy >= 10:
                    current_interval = poll_interval_slow
                else:
                    current_interval = poll_interval  # stay at base during transition
            elif state in ("UPSTREAM_LOAD_PRESSURE", "DOWNSTREAM_DEGRADATION"):
                consecutive_pressure += 1
                consecutive_healthy = 0
                current_interval = poll_interval_fast  # react faster under pressure
            else:
                current_interval = poll_interval       # base for PIPELINE_CEILING

            # ── Cooldown: prevent thrashing ───────────────────────────
            # If a scale action was applied this cycle, enforce a cooldown
            # before the next scale — prevents rapid up/down oscillation
            # that occurs when fast polling detects transient CPU spikes.
            now = time.monotonic()
            if actuator_report.applied():
                last_scale_time = now
                logger.debug("Scale applied — cooldown starts (%.0fs)", SCALE_COOLDOWN_SECONDS)

            # ── Improvement 2: SLO violation escalation ───────────────
            p99 = snapshot.p99_latency_ms
            if p99 and p99 > 500.0:
                consecutive_slo_violations += 1
                if consecutive_slo_violations >= slo_window:
                    logger.warning(
                        "SLO violated %d consecutive cycles (p99=%.0fms) "
                        "-- forcing fast interval",
                        consecutive_slo_violations, p99,
                    )
                    current_interval = poll_interval_fast
            else:
                consecutive_slo_violations = 0

        except Exception:
            logger.exception("Unhandled error in cycle %d -- skipping", cycle_number)
            current_interval = poll_interval

        # ── Drift-corrected sleep ─────────────────────────────────────
        elapsed    = time.monotonic() - cycle_start
        sleep_for  = max(0.0, current_interval - elapsed)

        if elapsed > current_interval:
            logger.warning("Cycle %d took %.1fs > interval %.0fs",
                           cycle_number, elapsed, current_interval)
        if running:
            time.sleep(sleep_for)

    decision_logger.close()
    logger.info("ACOMP controller stopped after %d cycles", cycle_number)
    return 0


if __name__ == "__main__":
    sys.exit(main())