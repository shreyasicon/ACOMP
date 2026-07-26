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
from acomp.explainability_api import ExplainabilityAPI
from acomp.adaptive_engine import AdaptiveEngine
from acomp.adaptive_engine import AdaptiveEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("acomp.main")


def read_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def main() -> int:
    logger.info("ACOMP controller starting (v4 — slope + EWMA + per-service cooldown + API + SLO prediction)")

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
    api_port            = int(read_env("ACOMP_API_PORT", "8080"))

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

    # ── Start Adaptive Engine ────────────────────────────────────────
    adaptive = AdaptiveEngine(config_path)
    logger.info("AdaptiveEngine started — mode=%s", adaptive.current_mode().name)

    # ── Start Adaptive Engine ────────────────────────────────────────
    adaptive = AdaptiveEngine(config_path)
    logger.info("AdaptiveEngine ready — mode=%s", adaptive.current_mode().name)

    # ── Start Explainability API ─────────────────────────────────────
    api = ExplainabilityAPI(decision_logger, port=api_port)
    api.start()

    # ── Graceful shutdown ────────────────────────────────────────────
    running = True

    def _shutdown(signum, frame):
        nonlocal running
        logger.info("Received signal %d -- shutting down after current cycle", signum)
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # ── Adaptive state (v4) ──────────────────────────────────────────
    cycle_number               = 0
    consecutive_pressure       = 0   # sustained pressure streak
    consecutive_healthy        = 0   # sustained healthy streak
    consecutive_slo_violations = 0   # p99 > 500ms streak
    prev_request_rate          = None
    current_interval           = poll_interval
    # Per-service cooldown: track last scale time per service independently
    # Allows fast response to a newly pressured service while cooling others
    last_scale_time: dict[str, float] = {}
    SCALE_COOLDOWN_SECONDS     = 30.0 # minimum seconds between scale actions per service

    logger.info(
        "ACOMP v4 starting — base=%.0fs fast=%.0fs slow=%.0fs "
        "per-service-cooldown=%.0fs EWMA+slope active",
        poll_interval, poll_interval_fast, poll_interval_slow, SCALE_COOLDOWN_SECONDS
    )

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

            # SLO trend alert: linear regression predicts breach within 2 cycles
            if snapshot.slo_trend_alert:
                rate_rising = True
                logger.warning(
                    "SLO trend alert: predicted p99=%.0fms -- activating pre-scale",
                    snapshot.predicted_p99_ms or 0
                )

            # ── Adaptive Engine evaluation ────────────────────────────
            ctx = adaptive.evaluate(snapshot)
            if ctx.pre_scale_signal:
                rate_rising = True
            if ctx.active_event:
                logger.info("Event active: %s — mode=%s",
                            ctx.active_event.name, ctx.effective_mode.name)
            # Pass frozen services and budget thresholds forward
            actuator.frozen_services = ctx.frozen_services
            policy_engine.budget_thresholds = ctx.budget_exhausted_services

            # ── Stage 2: Decide ───────────────────────────────────────
            decision_set, audit_record = policy_engine.run_cycle(
                snapshot, rate_rising_fast=rate_rising
            )

            # ── Stage 3: Actuate with per-service cooldown ───────────
            # Each service has its own cooldown timer — allows ACOMP to
            # rapidly scale a newly pressured service while leaving recently
            # scaled services in their cooldown window. Prevents v2 thrashing
            # while enabling fast response to new pressure signals.
            now_check = time.monotonic()
            if decision_set.actionable():
                cooled_decisions = []
                suppressed = []
                for decision in decision_set.actionable():
                    svc = decision.service_name
                    last_t = last_scale_time.get(svc, 0.0)
                    remaining = SCALE_COOLDOWN_SECONDS - (now_check - last_t)
                    if remaining > 0 and not snapshot.slope_signal:
                        # Suppress unless slope signal overrides cooldown
                        suppressed.append(svc)
                        logger.debug(
                            "Cooldown: %s (%.0fs remaining) -- suppressed",
                            svc, remaining
                        )
                    else:
                        cooled_decisions.append(decision)

                if suppressed:
                    logger.info(
                        "Per-service cooldown suppressed: %s", ", ".join(suppressed)
                    )

                # Apply only non-cooled decisions
                if cooled_decisions:
                    actuator_report = actuator.apply(decision_set)
                    # Update per-service cooldown timestamps for applied services
                    for d in cooled_decisions:
                        last_scale_time[d.service_name] = now_check
                else:
                    from acomp.actuator import ActuatorReport
                    actuator_report = ActuatorReport()
            else:
                actuator_report = actuator.apply(decision_set)

            # ── Stage 4: Log ──────────────────────────────────────────
            decision_logger.log(
                audit=audit_record,
                actuation=actuator_report,
                cycle_number=cycle_number,
            )

            # ── Improvement 1: Adaptive poll interval with hysteresis ────
            state = audit_record.pipeline_state
            slope_active = snapshot.slope_signal

            if state == "HEALTHY" and not slope_active:
                consecutive_pressure = 0
                consecutive_healthy += 1
                if consecutive_healthy >= 10:
                    current_interval = poll_interval_slow
                else:
                    current_interval = poll_interval
            elif state in ("UPSTREAM_LOAD_PRESSURE", "DOWNSTREAM_DEGRADATION") or slope_active:
                consecutive_pressure += 1
                consecutive_healthy = 0
                current_interval = poll_interval_fast  # fast poll when slope or pressure

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

    api.stop()
    adaptive._watcher.stop()
    decision_logger.close()
    logger.info("ACOMP controller stopped after %d cycles", cycle_number)
    return 0


if __name__ == "__main__":
    sys.exit(main())
