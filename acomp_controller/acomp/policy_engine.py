"""
acomp/policy_engine.py

The Policy Engine component of ACOMP. Implements Algorithm 1 (ACOMP Policy
Engine Cycle) from the MSc thesis exactly, operating deterministically on a
MetricSnapshot produced by the Collector and the dependency graph/guardrails
from the Context Map.

Per the thesis component specification table:
    Input:      MetricSnapshot from Collector + ContextMap
    Processing: Classifies pipeline state; computes scaling decisions
    Output:     DecisionSet + AuditRecord per cycle

The algorithm proceeds in three stages (matching the thesis Explanation block):

  Stage 1 (Lines 1-11): State classification
      Each service is checked against CPU and request-rate thresholds.
      If a service is under pressure, latency/throughput patterns distinguish
      UPSTREAM_LOAD_PRESSURE from DOWNSTREAM_DEGRADATION. This answers SQ2 --
      the two causally distinct conditions are never treated identically.

  Stage 2 (Lines 12-20): Horizontal scaling computation
      For UPSTREAM_LOAD_PRESSURE, the root-cause service replica delta is
      computed using the standard Kubernetes HPA target-utilisation formula
      (Equation 5 in the thesis). Downstream services are then pre-adjusted
      using work factors (Equation 3/4), with guardrail enforcement.

  Stage 3 (Line 21): Audit record construction
      Every decision, rejection, and the full reasoning path are recorded
      in a structured JSON-serialisable AuditRecord, directly addressing SQ4.

The Policy Engine is purely deterministic: given the same MetricSnapshot and
ContextMap, it always produces the same DecisionSet and AuditRecord. This
reproducibility is a deliberate design choice for operational explainability,
contrasting with the stochastic policies of RL-based controllers.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from .collector import MetricSnapshot, ServiceMetrics
from .context_map import ContextMap

logger = logging.getLogger("acomp.policy_engine")

# ----------------------------------------------------------------------
# Thresholds (operator-configurable via alomp_config.yaml in future;
# for now, these match the thesis evaluation scenario parameters)
# ----------------------------------------------------------------------

# theta_cpu: CPU utilisation above which a service is considered under pressure.
# Matches the 70% target used in the Baseline A (HPA only) comparator, so
# ACOMP triggers at the same CPU level, making the comparison fair.
CPU_PRESSURE_THRESHOLD = 0.70

# theta_spike: absolute request rate (req/s) above which traffic is considered
# a genuine spike. Based on observed Locust traffic (3-5 req/s at 10 users,
# scaling proportionally), a spike scenario at 50+ users would drive 15-20 req/s.
# We set 10 req/s as the threshold: clearly above normal baseline (3-5 req/s)
# but well below a genuine bursty scenario (20+ req/s in Scenario 1).
# This is calibrated against your live Collector output (3.27 req/s observed).
REQUEST_RATE_SPIKE_THRESHOLD = 10.0

# Latency threshold above which p99 is considered "high" relative to the
# SLO target of 500ms (from thesis Evaluation section, Table S1-S3).
LATENCY_HIGH_THRESHOLD_MS = 500.0

# ----------------------------------------------------------------------
# Pipeline state enumeration (four states from thesis Section 4.1)
# ----------------------------------------------------------------------

class PipelineState(str, Enum):
    HEALTHY = "HEALTHY"
    UPSTREAM_LOAD_PRESSURE = "UPSTREAM_LOAD_PRESSURE"
    DOWNSTREAM_DEGRADATION = "DOWNSTREAM_DEGRADATION"
    PIPELINE_CEILING = "PIPELINE_CEILING"


# ----------------------------------------------------------------------
# Decision types
# ----------------------------------------------------------------------

class DecisionOutcome(str, Enum):
    SCALE_UP = "SCALE_UP"
    GUARDRAIL_CLAMPED = "GUARDRAIL_CLAMPED"
    SUPPRESSED = "SUPPRESSED"          # below propagation threshold
    NO_ACTION = "NO_ACTION"            # HEALTHY or DOWNSTREAM_DEGRADATION
    ALERT = "ALERT"                    # DOWNSTREAM_DEGRADATION -- alert only, no scaling


@dataclass
class ScalingDecision:
    """A single scaling decision for one service in one cycle."""
    service: str
    outcome: DecisionOutcome
    current_replicas: int
    delta: int = 0                      # positive = scale up
    target_replicas: int = 0
    reason: str = ""


@dataclass
class DecisionSet:
    """All scaling decisions produced in one cycle."""
    decisions: list[ScalingDecision] = field(default_factory=list)

    def add(self, decision: ScalingDecision) -> None:
        self.decisions.append(decision)

    def actionable(self) -> list[ScalingDecision]:
        """Returns only SCALE_UP decisions -- the ones the Actuator
        will actually apply to the Kubernetes API."""
        return [d for d in self.decisions if d.outcome == DecisionOutcome.SCALE_UP]


@dataclass
class AuditRecord:
    """
    The structured JSON-serialisable audit record produced per cycle,
    matching the Decision Log Format defined in the thesis (Listing:
    Decision Logger JSON Lines output for one ACOMP control cycle).

    Every evaluation data point must trace to a specific record,
    directly addressing SQ4 (operational explainability).
    """
    timestamp: str
    pipeline_state: str
    root_cause_service: Optional[str]
    decisions: list[dict]
    rejected: list[dict]
    reasoning: str
    cycle_duration_ms: float = 0.0

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "pipeline_state": self.pipeline_state,
            "root_cause_service": self.root_cause_service,
            "decisions": self.decisions,
            "rejected": self.rejected,
            "reasoning": self.reasoning,
            "cycle_duration_ms": self.cycle_duration_ms,
        }


# ----------------------------------------------------------------------
# Policy Engine
# ----------------------------------------------------------------------

class PolicyEngine:
    """
    Implements Algorithm 1 (ACOMP Policy Engine Cycle) from the thesis.

    Usage:
        engine = PolicyEngine(context_map=context_map)
        decision_set, audit_record = engine.run_cycle(snapshot)
    """

    def __init__(self, context_map: ContextMap):
        self.context_map = context_map

    def run_cycle(
        self, snapshot: MetricSnapshot
    ) -> tuple[DecisionSet, AuditRecord]:
        """
        Executes one full Policy Engine cycle as per Algorithm 1.

        Returns a (DecisionSet, AuditRecord) tuple. The DecisionSet contains
        all decisions including suppressed and guardrail-clamped ones, so
        the Actuator only needs to execute decision_set.actionable(). The
        AuditRecord contains the full reasoning path for the Decision Logger.
        """
        import time
        cycle_start = time.monotonic()

        decision_set = DecisionSet()
        rejected: list[dict] = []

        # ------------------------------------------------------------------
        # Stage 1: State Classification (Algorithm 1, Lines 1-11)
        # ------------------------------------------------------------------

        state = PipelineState.HEALTHY
        root_cause: Optional[str] = None
        reasoning_lines: list[str] = []

        for name in self.context_map.service_names():
            metrics = snapshot.get(name)
            if metrics is None:
                continue

            cpu = metrics.cpu_utilisation
            req_rate = metrics.request_rate
            latency = metrics.latency_p99_ms

            cpu_pressure = cpu is not None and cpu > CPU_PRESSURE_THRESHOLD

            if not cpu_pressure:
                continue

            # Service is under CPU pressure -- use latency to distinguish cause.
            # DOWNSTREAM_DEGRADATION: high latency (> SLO threshold) with normal
            # request rate indicates requests are queuing due to a downstream
            # bottleneck, not more requests arriving. No scaling should occur.
            # UPSTREAM_LOAD_PRESSURE: high CPU without extreme latency, or with
            # a genuine request rate increase -- scale up.
            latency_high = latency is not None and latency > LATENCY_HIGH_THRESHOLD_MS
            # "throughput normal" means request rate is not significantly elevated --
            # if we have no request rate data (internal services), we rely solely
            # on the latency signal, which is conservative and correct.
            throughput_normal = req_rate is None or req_rate <= REQUEST_RATE_SPIKE_THRESHOLD

            if latency_high and throughput_normal:
                # Lines 5-7: Downstream degradation detected.
                state = PipelineState.DOWNSTREAM_DEGRADATION
                root_cause = name
                reasoning_lines.append(
                    f"{name}: CPU={_fmt_pct(cpu)}, p99={_fmt_ms(latency)} "
                    f"-- high latency without request spike indicates "
                    f"DOWNSTREAM_DEGRADATION, root cause attributed here"
                )
                logger.warning(
                    "DOWNSTREAM_DEGRADATION detected at %s "
                    "(cpu=%.1f%%, p99=%.0fms) -- suppressing all scaling, alerting only",
                    name, (cpu or 0) * 100, latency or 0,
                )
                break  # No scaling -- only alert. Stop checking other services.

            else:
                # Lines 8-10: Upstream load pressure.
                state = PipelineState.UPSTREAM_LOAD_PRESSURE
                root_cause = name
                reasoning_lines.append(
                    f"{name}: CPU={_fmt_pct(cpu)}, req_rate={_fmt_rps(req_rate)} "
                    f"-- exceeds threshold, classified as UPSTREAM_LOAD_PRESSURE"
                )
                logger.info(
                    "UPSTREAM_LOAD_PRESSURE at %s (cpu=%.1f%%, req_rate=%.2f req/s)",
                    name, (cpu or 0) * 100, req_rate or 0,
                )
                break  # Root cause found -- proceed to scaling stage.

        # ------------------------------------------------------------------
        # Stage 2: Horizontal Scaling Computation (Algorithm 1, Lines 12-20)
        # ------------------------------------------------------------------

        if state == PipelineState.DOWNSTREAM_DEGRADATION:
            # Alert only -- no scaling. Record as a single ALERT decision.
            decision_set.add(ScalingDecision(
                service=root_cause or "unknown",
                outcome=DecisionOutcome.ALERT,
                current_replicas=_replicas(snapshot, root_cause),
                delta=0,
                reason="DOWNSTREAM_DEGRADATION: suppressing all scaling actions",
            ))
            reasoning_lines.append(
                "Policy: DOWNSTREAM_DEGRADATION state -- no scaling issued. "
                "Operator alert only. Check downstream service health."
            )

        elif state == PipelineState.UPSTREAM_LOAD_PRESSURE and root_cause is not None:
            root_metrics = snapshot.get(root_cause)
            r_root = _replicas(snapshot, root_cause)
            cpu_root = root_metrics.cpu_utilisation if root_metrics else None

            # Line 13: Compute root-cause replica delta using the standard
            # Kubernetes HPA target-utilisation formula (Equation 5):
            #   delta_root = ceil(r_root * cpu_root / theta_cpu) - r_root
            if cpu_root is not None and r_root > 0:
                desired = math.ceil(r_root * cpu_root / CPU_PRESSURE_THRESHOLD)
                delta_root = max(0, desired - r_root)
            else:
                delta_root = 1  # Fallback: add one replica if CPU data missing
                reasoning_lines.append(
                    f"{root_cause}: CPU data unavailable, defaulting delta=1"
                )

            reasoning_lines.append(
                f"{root_cause}: HPA formula -> "
                f"ceil({r_root} * {_fmt_pct(cpu_root)} / {CPU_PRESSURE_THRESHOLD:.0%}) "
                f"- {r_root} = delta {delta_root}"
            )

            # Line 14: Add root-cause decision to DecisionSet.
            target_root = r_root + delta_root
            if target_root <= self.context_map.guardrails.max_replicas:
                decision_set.add(ScalingDecision(
                    service=root_cause,
                    outcome=DecisionOutcome.SCALE_UP,
                    current_replicas=r_root,
                    delta=delta_root,
                    target_replicas=target_root,
                    reason=f"Root cause: HPA formula delta={delta_root}",
                ))
            else:
                # Root cause itself hits guardrail -- clamp and set PIPELINE_CEILING
                clamped_target = self.context_map.guardrails.max_replicas
                clamped_delta = clamped_target - r_root
                state = PipelineState.PIPELINE_CEILING
                decision_set.add(ScalingDecision(
                    service=root_cause,
                    outcome=DecisionOutcome.GUARDRAIL_CLAMPED,
                    current_replicas=r_root,
                    delta=clamped_delta,
                    target_replicas=clamped_target,
                    reason=f"Guardrail: max_replicas={self.context_map.guardrails.max_replicas}",
                ))
                reasoning_lines.append(
                    f"{root_cause}: guardrail hit at root cause -- "
                    f"PIPELINE_CEILING, clamped to {clamped_target}"
                )

            # Lines 15-20: Propagate to downstream dependencies using work factors.
            for dep in self.context_map.downstream_of(root_cause):
                r_d = _replicas(snapshot, dep.service)
                wf = dep.work_factor

                # Equation 4: delta_d = floor(delta_root * W(root, d))
                delta_d = math.floor(delta_root * wf)

                # Line 16: Suppress if below propagation threshold theta_prop
                if delta_d <= self.context_map.guardrails.propagation_threshold:
                    rejected.append({
                        "service": dep.service,
                        "reason": "SUPPRESSED",
                        "delta_computed": delta_d,
                        "threshold": self.context_map.guardrails.propagation_threshold,
                    })
                    reasoning_lines.append(
                        f"{dep.service}: delta {delta_d} < "
                        f"theta_prop {self.context_map.guardrails.propagation_threshold} "
                        f"-- suppressed"
                    )
                    decision_set.add(ScalingDecision(
                        service=dep.service,
                        outcome=DecisionOutcome.SUPPRESSED,
                        current_replicas=r_d,
                        delta=delta_d,
                        reason=f"Below propagation threshold ({delta_d} <= "
                               f"{self.context_map.guardrails.propagation_threshold})",
                    ))
                    continue

                # Lines 17-20: Guardrail check on downstream
                target_d = r_d + delta_d
                if target_d <= self.context_map.guardrails.max_replicas:
                    decision_set.add(ScalingDecision(
                        service=dep.service,
                        outcome=DecisionOutcome.SCALE_UP,
                        current_replicas=r_d,
                        delta=delta_d,
                        target_replicas=target_d,
                        reason=f"Work factor propagation: W({root_cause},{dep.service})="
                               f"{wf}, delta={delta_d}",
                    ))
                    reasoning_lines.append(
                        f"{dep.service}: W={wf}, delta={delta_d} -> "
                        f"scale {r_d} -> {target_d}"
                    )
                else:
                    # Guardrail clamped downstream -- record rejection, set CEILING
                    clamped_target_d = self.context_map.guardrails.max_replicas
                    state = PipelineState.PIPELINE_CEILING
                    rejected.append({
                        "service": dep.service,
                        "reason": "GUARDRAIL_CLAMPED",
                        "delta_computed": delta_d,
                        "max_replicas": self.context_map.guardrails.max_replicas,
                    })
                    decision_set.add(ScalingDecision(
                        service=dep.service,
                        outcome=DecisionOutcome.GUARDRAIL_CLAMPED,
                        current_replicas=r_d,
                        delta=clamped_target_d - r_d,
                        target_replicas=clamped_target_d,
                        reason=f"Guardrail: max_replicas={self.context_map.guardrails.max_replicas}",
                    ))
                    reasoning_lines.append(
                        f"{dep.service}: guardrail hit -> PIPELINE_CEILING, "
                        f"clamped to {clamped_target_d}"
                    )

        else:
            # HEALTHY -- no action
            reasoning_lines.append("All services within thresholds -- HEALTHY, no action")

        # ------------------------------------------------------------------
        # Stage 3: Audit Record Construction (Algorithm 1, Line 21)
        # ------------------------------------------------------------------
        cycle_duration_ms = (time.monotonic() - cycle_start) * 1000

        audit = AuditRecord(
            timestamp=snapshot.timestamp.isoformat(),
            pipeline_state=state.value,
            root_cause_service=root_cause,
            decisions=[
                {
                    "service": d.service,
                    "outcome": d.outcome.value,
                    "current_replicas": d.current_replicas,
                    "delta": d.delta,
                    "target_replicas": d.target_replicas,
                    "reason": d.reason,
                }
                for d in decision_set.decisions
            ],
            rejected=rejected,
            reasoning=" | ".join(reasoning_lines) if reasoning_lines else "HEALTHY",
            cycle_duration_ms=round(cycle_duration_ms, 2),
        )

        logger.info(
            "Cycle complete: state=%s, root=%s, actions=%d, rejected=%d, "
            "duration=%.1fms",
            state.value, root_cause,
            len(decision_set.actionable()), len(rejected),
            cycle_duration_ms,
        )

        return decision_set, audit


# ----------------------------------------------------------------------
# Formatting helpers
# ----------------------------------------------------------------------

def _fmt_pct(value: float | None) -> str:
    return f"{value:.1%}" if value is not None else "N/A"

def _fmt_ms(value: float | None) -> str:
    return f"{value:.0f}ms" if value is not None else "N/A"

def _fmt_rps(value: float | None) -> str:
    return f"{value:.2f} req/s" if value is not None else "N/A"

def _replicas(snapshot: MetricSnapshot, service: str | None) -> int:
    """Returns the current replica count for a service, defaulting to 1
    if the service is unknown or data is unavailable."""
    if service is None:
        return 1
    m = snapshot.get(service)
    if m is None or m.replica_count is None:
        return 1
    return m.replica_count
