"""
test_actuator_offline.py

Offline unit tests for the ACOMP Actuator. All Kubernetes API calls are
mocked so no cluster connection is required.

Run with: python test_actuator_offline.py
"""

import sys
from unittest.mock import MagicMock, patch

from acomp.actuator import Actuator, ActuationStatus
from acomp.context_map import ContextMap, ServiceNode, Dependency, Guardrails
from acomp.policy_engine import (
    DecisionSet, ScalingDecision, DecisionOutcome
)


def make_context_map() -> ContextMap:
    return ContextMap(
        services=[
            ServiceNode(
                name="frontend",
                downstream=[Dependency(service="cartservice", work_factor=0.60)],
            ),
            ServiceNode(name="cartservice", downstream=[]),
        ],
        guardrails=Guardrails(min_replicas=1, max_replicas=10,
                              propagation_threshold=0.30),
    )


def make_decision_set(*decisions: ScalingDecision) -> DecisionSet:
    ds = DecisionSet()
    for d in decisions:
        ds.add(d)
    return ds


CM = make_context_map()


# ----------------------------------------------------------------------
# Test 1: Dry run -- no real API calls, correct status
# ----------------------------------------------------------------------

def test_dry_run_does_not_call_api():
    actuator = Actuator(namespace="default", context_map=CM, dry_run=True)
    ds = make_decision_set(
        ScalingDecision(
            service="frontend",
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=2,
            delta=1,
            target_replicas=3,
            reason="test",
        )
    )

    # If any real API call is made this will fail since no kubernetes client
    # is initialised in dry_run mode
    report = actuator.apply(ds)

    assert len(report.results) == 1
    assert report.results[0].status == ActuationStatus.DRY_RUN
    assert report.results[0].target_replicas == 3
    assert report.results[0].previous_replicas == 2
    print("PASS: test_dry_run_does_not_call_api")


# ----------------------------------------------------------------------
# Test 2: Idempotency -- skip if already at target replicas
# ----------------------------------------------------------------------

def test_skips_if_already_at_target():
    actuator = Actuator(namespace="default", context_map=CM, dry_run=False)

    # Mock the Kubernetes client
    mock_api = MagicMock()
    mock_scale = MagicMock()
    mock_scale.spec.replicas = 3   # cluster already at target
    mock_api.read_namespaced_deployment_scale.return_value = mock_scale
    actuator._k8s_apps_v1 = mock_api

    ds = make_decision_set(
        ScalingDecision(
            service="frontend",
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=3,
            delta=0,
            target_replicas=3,
            reason="test",
        )
    )
    report = actuator.apply(ds)

    assert report.results[0].status == ActuationStatus.SKIPPED
    mock_api.patch_namespaced_deployment_scale.assert_not_called()
    print("PASS: test_skips_if_already_at_target")


# ----------------------------------------------------------------------
# Test 3: Successfully applies patch when replicas differ
# ----------------------------------------------------------------------

def test_applies_patch_when_needed():
    actuator = Actuator(namespace="default", context_map=CM, dry_run=False)

    # Mock the internal methods directly to avoid kubernetes import dependency
    actuator._get_current_replicas = MagicMock(return_value=2)
    patch_mock = MagicMock()
    actuator._patch_replicas = patch_mock
    # Mark client as initialised so apply() doesn't try to load k8s config
    actuator._k8s_apps_v1 = MagicMock()

    ds = make_decision_set(
        ScalingDecision(
            service="frontend",
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=2,
            delta=1,
            target_replicas=3,
            reason="test",
        )
    )
    report = actuator.apply(ds)

    assert report.results[0].status == ActuationStatus.APPLIED
    assert report.results[0].target_replicas == 3
    patch_mock.assert_called_once_with("frontend", 3)
    print("PASS: test_applies_patch_when_needed")


# ----------------------------------------------------------------------
# Test 4: Hard cap safety net -- enforced even if Policy Engine missed it
# ----------------------------------------------------------------------

def test_hard_cap_clamps_target():
    actuator = Actuator(
        namespace="default", context_map=CM,
        dry_run=True,
        max_replicas_hard_cap=10,
    )
    ds = make_decision_set(
        ScalingDecision(
            service="frontend",
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=9,
            delta=5,
            target_replicas=14,   # exceeds hard cap of 10
            reason="test",
        )
    )
    report = actuator.apply(ds)

    # Should be clamped to 10
    assert report.results[0].target_replicas == 10
    print("PASS: test_hard_cap_clamps_target")


# ----------------------------------------------------------------------
# Test 5: API failure handled gracefully -- returns FAILED status
# ----------------------------------------------------------------------

def test_api_failure_returns_failed_status():
    actuator = Actuator(namespace="default", context_map=CM, dry_run=False)

    mock_api = MagicMock()
    mock_api.read_namespaced_deployment_scale.side_effect = \
        Exception("connection refused")
    actuator._k8s_apps_v1 = mock_api

    ds = make_decision_set(
        ScalingDecision(
            service="frontend",
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=2,
            delta=1,
            target_replicas=3,
            reason="test",
        )
    )
    report = actuator.apply(ds)

    assert report.results[0].status == ActuationStatus.FAILED
    assert "connection refused" in report.results[0].error
    assert len(report.failed()) == 1
    print("PASS: test_api_failure_returns_failed_status")


# ----------------------------------------------------------------------
# Test 6: Dependency ordering -- upstream applied before downstream
# ----------------------------------------------------------------------

def test_dependency_ordering_upstream_first():
    actuator = Actuator(namespace="default", context_map=CM, dry_run=True)

    # Both frontend and cartservice need scaling
    ds = make_decision_set(
        ScalingDecision(
            service="cartservice",    # downstream -- added first to test ordering
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=1,
            delta=1,
            target_replicas=2,
            reason="propagated",
        ),
        ScalingDecision(
            service="frontend",      # upstream -- should be applied first
            outcome=DecisionOutcome.SCALE_UP,
            current_replicas=2,
            delta=2,
            target_replicas=4,
            reason="root cause",
        ),
    )
    report = actuator.apply(ds)

    # Check ordering: frontend (upstream, index 0 in context map)
    # must appear before cartservice (downstream, index 1)
    services_in_order = [r.service for r in report.results]
    frontend_idx = services_in_order.index("frontend")
    cartservice_idx = services_in_order.index("cartservice")
    assert frontend_idx < cartservice_idx, \
        f"Expected frontend before cartservice, got order: {services_in_order}"
    print("PASS: test_dependency_ordering_upstream_first")


# ----------------------------------------------------------------------
# Test 7: Empty decision set -- no API calls, empty report
# ----------------------------------------------------------------------

def test_empty_decision_set():
    actuator = Actuator(namespace="default", context_map=CM, dry_run=True)
    ds = DecisionSet()  # no decisions
    report = actuator.apply(ds)

    assert len(report.results) == 0
    assert len(report.applied()) == 0
    print("PASS: test_empty_decision_set")


# ----------------------------------------------------------------------
# Run all tests
# ----------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_dry_run_does_not_call_api,
        test_skips_if_already_at_target,
        test_applies_patch_when_needed,
        test_hard_cap_clamps_target,
        test_api_failure_returns_failed_status,
        test_dependency_ordering_upstream_first,
        test_empty_decision_set,
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
          f"Actuator tests passed.")
    sys.exit(failed)
