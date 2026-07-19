"""
test_policy_engine.py

Offline unit tests for the ACOMP Policy Engine (Algorithm 1). Tests all four
pipeline state classifications and the scaling computation logic using synthetic
MetricSnapshots matching the structure your live Collector produces.

Run with: python test_policy_engine.py
(no cluster or Prometheus connection required)
"""

import math
import sys
from datetime import datetime, timezone

from acomp.collector import MetricSnapshot, ServiceMetrics
from acomp.context_map import ContextMap, ServiceNode, Dependency, Guardrails
from acomp.policy_engine import (
    PolicyEngine, PipelineState, DecisionOutcome,
)


# ----------------------------------------------------------------------
# Test fixture helpers
# ----------------------------------------------------------------------

def make_snapshot(**services) -> MetricSnapshot:
    """
    Builds a MetricSnapshot from keyword args.
    Each value is a dict with optional keys:
        cpu, replicas, request_rate, latency_p99_ms, error_rate
    """
    snap = MetricSnapshot(timestamp=datetime.now(timezone.utc))
    for name, vals in services.items():
        snap.services[name] = ServiceMetrics(
            name=name,
            cpu_utilisation=vals.get("cpu"),
            replica_count=vals.get("replicas", 1),
            request_rate=vals.get("request_rate"),
            latency_p99_ms=vals.get("latency_p99_ms"),
            error_rate=vals.get("error_rate"),
        )
    return snap


def make_context_map() -> ContextMap:
    """
    Simplified two-level dependency graph for testing:
        frontend -> cartservice (W=0.60)
        frontend -> productcatalogservice (W=0.40)
    Guardrails: max_replicas=10, propagation_threshold=0.30
    """
    return ContextMap(
        services=[
            ServiceNode(
                name="frontend",
                downstream=[
                    Dependency(service="cartservice", work_factor=0.60),
                    Dependency(service="productcatalogservice", work_factor=0.40),
                ],
            ),
            ServiceNode(name="cartservice", downstream=[]),
            ServiceNode(name="productcatalogservice", downstream=[]),
        ],
        guardrails=Guardrails(
            min_replicas=1,
            max_replicas=10,
            propagation_threshold=0.30,
        ),
    )


engine = PolicyEngine(context_map=make_context_map())


# ----------------------------------------------------------------------
# Test 1: HEALTHY -- no action when all services are within thresholds
# ----------------------------------------------------------------------

def test_healthy_state():
    snapshot = make_snapshot(
        frontend={"cpu": 0.30, "replicas": 2, "request_rate": 5.0, "latency_p99_ms": 120},
        cartservice={"cpu": 0.20, "replicas": 1},
        productcatalogservice={"cpu": 0.25, "replicas": 1},
    )
    ds, audit = engine.run_cycle(snapshot, rate_rising_fast=False)

    assert audit.pipeline_state == PipelineState.HEALTHY.value, \
        f"Expected HEALTHY, got {audit.pipeline_state}"
    assert len(ds.actionable()) == 0, \
        f"Expected 0 actionable decisions in HEALTHY, got {len(ds.actionable())}"
    assert audit.root_cause_service is None
    print("PASS: test_healthy_state")


# ----------------------------------------------------------------------
# Test 2: UPSTREAM_LOAD_PRESSURE -- root cause scales via HPA formula,
# downstream pre-adjusted via work factors
# ----------------------------------------------------------------------

def test_upstream_load_pressure_with_propagation():
    """
    Frontend at 80% CPU (above 70% threshold), 1 current replica.
    Expected:
        delta_root = ceil(1 * 0.80 / 0.70) - 1 = ceil(1.142) - 1 = 2 - 1 = 1
        cartservice: delta = floor(1 * 0.60) = 0 -- suppressed (< threshold 0.30)
          Wait -- 0 < 0.30 so suppressed.
        productcatalogservice: delta = floor(1 * 0.40) = 0 -- suppressed.
    So only frontend should scale up.
    """
    snapshot = make_snapshot(
        frontend={"cpu": 0.80, "replicas": 1, "request_rate": 5.0, "latency_p99_ms": 200},
        cartservice={"cpu": 0.20, "replicas": 1},
        productcatalogservice={"cpu": 0.15, "replicas": 1},
    )
    ds, audit = engine.run_cycle(snapshot, rate_rising_fast=False)

    assert audit.pipeline_state == PipelineState.UPSTREAM_LOAD_PRESSURE.value, \
        f"Expected UPSTREAM_LOAD_PRESSURE, got {audit.pipeline_state}"
    assert audit.root_cause_service == "frontend"

    actionable = ds.actionable()
    assert len(actionable) == 1, f"Expected 1 actionable, got {len(actionable)}"
    assert actionable[0].service == "frontend"
    assert actionable[0].delta == 1
    assert actionable[0].target_replicas == 2

    print("PASS: test_upstream_load_pressure_with_propagation")


def test_upstream_load_pressure_propagates_to_downstream():
    """
    Frontend at 90% CPU with 3 current replicas -- delta_root should be larger,
    allowing propagation to downstream services.
    delta_root = ceil(3 * 0.90 / 0.70) - 3 = ceil(3.857) - 3 = 4 - 3 = 1
    cartservice: floor(1 * 0.60) = 0 -- suppressed (0 < 0.30)
    productcatalogservice: floor(1 * 0.40) = 0 -- suppressed

    With 5 replicas and 90% CPU:
    delta_root = ceil(5 * 0.90 / 0.70) - 5 = ceil(6.428) - 5 = 7 - 5 = 2
    cartservice: floor(2 * 0.60) = 1 -- 1 > 0.30 so SCALE_UP
    productcatalogservice: floor(2 * 0.40) = 0 -- suppressed
    """
    snapshot = make_snapshot(
        frontend={"cpu": 0.90, "replicas": 5, "request_rate": 20.0, "latency_p99_ms": 280},
        cartservice={"cpu": 0.40, "replicas": 3},
        productcatalogservice={"cpu": 0.30, "replicas": 2},
    )
    ds, audit = engine.run_cycle(snapshot, rate_rising_fast=False)

    assert audit.pipeline_state == PipelineState.UPSTREAM_LOAD_PRESSURE.value
    assert audit.root_cause_service == "frontend"

    actionable = ds.actionable()
    services_scaled = {d.service for d in actionable}

    # Frontend must be in actionable
    assert "frontend" in services_scaled, \
        f"frontend not in scaled services: {services_scaled}"

    # Verify frontend delta mathematically
    frontend_decision = next(d for d in actionable if d.service == "frontend")
    expected_delta = math.ceil(5 * 0.90 / 0.70) - 5
    assert frontend_decision.delta == expected_delta, \
        f"Expected frontend delta={expected_delta}, got {frontend_decision.delta}"

    # cartservice should have been propagated (delta=1 > 0.30)
    assert "cartservice" in services_scaled, \
        f"cartservice should be propagated, got: {services_scaled}"
    cart_decision = next(d for d in actionable if d.service == "cartservice")
    expected_cart_delta = math.floor(expected_delta * 0.60)
    assert cart_decision.delta == expected_cart_delta, \
        f"Expected cart delta={expected_cart_delta}, got {cart_decision.delta}"

    print("PASS: test_upstream_load_pressure_propagates_to_downstream")


# ----------------------------------------------------------------------
# Test 3: DOWNSTREAM_DEGRADATION -- no scaling, alert only
# ----------------------------------------------------------------------

def test_downstream_degradation_suppresses_scaling():
    """
    Frontend shows high CPU AND high latency but NO request rate spike.
    This is the signature of downstream degradation (latency backing up
    through the pipeline) rather than genuine load pressure.
    ACOMP must NOT scale -- scaling would make things worse.
    """
    snapshot = make_snapshot(
        frontend={
            "cpu": 0.78,
            "replicas": 2,
            "request_rate": 5.0,    # normal traffic, NOT spiking
            "latency_p99_ms": 850,  # very high latency -- downstream problem
        },
        cartservice={"cpu": 0.20, "replicas": 1},
        productcatalogservice={"cpu": 0.15, "replicas": 1},
    )
    ds, audit = engine.run_cycle(snapshot, rate_rising_fast=False)

    assert audit.pipeline_state == PipelineState.DOWNSTREAM_DEGRADATION.value, \
        f"Expected DOWNSTREAM_DEGRADATION, got {audit.pipeline_state}"

    # Critical: no SCALE_UP decisions should exist
    assert len(ds.actionable()) == 0, \
        f"DOWNSTREAM_DEGRADATION must suppress all scaling, got {len(ds.actionable())} actions"

    # Should have exactly one ALERT decision
    alert_decisions = [d for d in ds.decisions if d.outcome.value == "ALERT"]
    assert len(alert_decisions) == 1, \
        f"Expected 1 ALERT decision, got {len(alert_decisions)}"

    print("PASS: test_downstream_degradation_suppresses_scaling")


# ----------------------------------------------------------------------
# Test 4: PIPELINE_CEILING -- guardrail hit, state escalates
# ----------------------------------------------------------------------

def test_pipeline_ceiling_when_guardrail_hit():
    """
    Frontend at high CPU with 9 current replicas (max is 10).
    delta_root = ceil(9 * 0.85 / 0.70) - 9 = ceil(10.928) - 9 = 11 - 9 = 2
    target = 9 + 2 = 11 > max_replicas(10) -> GUARDRAIL_CLAMPED -> PIPELINE_CEILING
    """
    snapshot = make_snapshot(
        frontend={"cpu": 0.85, "replicas": 9, "request_rate": 40.0, "latency_p99_ms": 300},
        cartservice={"cpu": 0.50, "replicas": 5},
        productcatalogservice={"cpu": 0.45, "replicas": 4},
    )
    ds, audit = engine.run_cycle(snapshot, rate_rising_fast=False)

    assert audit.pipeline_state == PipelineState.PIPELINE_CEILING.value, \
        f"Expected PIPELINE_CEILING, got {audit.pipeline_state}"

    # There should be a GUARDRAIL_CLAMPED decision for frontend
    clamped = [d for d in ds.decisions if d.outcome == DecisionOutcome.GUARDRAIL_CLAMPED]
    assert len(clamped) >= 1, "Expected at least one GUARDRAIL_CLAMPED decision"
    assert clamped[0].target_replicas == 10  # clamped to max

    print("PASS: test_pipeline_ceiling_when_guardrail_hit")


# ----------------------------------------------------------------------
# Test 5: Audit record completeness -- SQ4
# ----------------------------------------------------------------------

def test_audit_record_is_complete():
    """
    Every audit record must contain all required fields for SQ4 compliance.
    Tests with UPSTREAM_LOAD_PRESSURE which produces the richest audit record.
    """
    snapshot = make_snapshot(
        frontend={"cpu": 0.80, "replicas": 5, "request_rate": 20.0, "latency_p99_ms": 200},
        cartservice={"cpu": 0.30, "replicas": 2},
        productcatalogservice={"cpu": 0.20, "replicas": 1},
    )
    ds, audit = engine.run_cycle(snapshot, rate_rising_fast=False)

    record = audit.to_dict()

    # All required top-level keys per thesis Decision Log Format
    required_keys = {"timestamp", "pipeline_state", "root_cause_service",
                     "decisions", "rejected", "reasoning", "cycle_duration_ms"}
    missing = required_keys - set(record.keys())
    assert not missing, f"Audit record missing keys: {missing}"

    # Timestamp must be ISO format
    assert "T" in record["timestamp"], "timestamp must be ISO format"

    # Reasoning must be non-empty
    assert record["reasoning"], "reasoning must not be empty"

    # Cycle duration must be positive
    assert record["cycle_duration_ms"] > 0, "cycle_duration_ms must be positive"

    print("PASS: test_audit_record_is_complete")


# ----------------------------------------------------------------------
# Test 6: Determinism -- same input always produces same output (SQ4)
# ----------------------------------------------------------------------

def test_policy_engine_is_deterministic():
    """
    Given the same MetricSnapshot, the Policy Engine must always produce
    the identical DecisionSet and AuditRecord. This is a core design
    guarantee contrasting ACOMP with RL-based controllers.
    """
    snapshot = make_snapshot(
        frontend={"cpu": 0.82, "replicas": 3, "request_rate": 15.0, "latency_p99_ms": 220},
        cartservice={"cpu": 0.35, "replicas": 2},
        productcatalogservice={"cpu": 0.25, "replicas": 1},
    )

    ds1, audit1 = engine.run_cycle(snapshot, rate_rising_fast=False)
    ds2, audit2 = engine.run_cycle(snapshot, rate_rising_fast=False)

    assert audit1.pipeline_state == audit2.pipeline_state
    assert audit1.root_cause_service == audit2.root_cause_service
    assert len(ds1.actionable()) == len(ds2.actionable())

    for d1, d2 in zip(ds1.actionable(), ds2.actionable()):
        assert d1.service == d2.service
        assert d1.delta == d2.delta
        assert d1.target_replicas == d2.target_replicas

    print("PASS: test_policy_engine_is_deterministic")


# ----------------------------------------------------------------------
# Run all tests
# ----------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_healthy_state,
        test_upstream_load_pressure_with_propagation,
        test_upstream_load_pressure_propagates_to_downstream,
        test_downstream_degradation_suppresses_scaling,
        test_pipeline_ceiling_when_guardrail_hit,
        test_audit_record_is_complete,
        test_policy_engine_is_deterministic,
    ]

    failed = 0
    for test in tests:
        try:
            test()
        except AssertionError as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR: {test.__name__}: {e}")
            failed += 1

    print(f"\n{'All' if not failed else str(len(tests)-failed)}/{len(tests)} "
          f"Policy Engine tests passed.")
    sys.exit(failed)
