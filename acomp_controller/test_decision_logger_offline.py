"""
test_decision_logger_offline.py

Offline unit tests for the ACOMP Decision Logger.
Run with: python test_decision_logger_offline.py
"""

import io
import json
import sys
from datetime import datetime, timezone

from acomp.actuator import ActuatorReport, ActuationResult, ActuationStatus
from acomp.decision_logger import DecisionLogger
from acomp.policy_engine import AuditRecord


def make_audit(state="HEALTHY") -> AuditRecord:
    return AuditRecord(
        timestamp=datetime.now(timezone.utc).isoformat(),
        pipeline_state=state,
        root_cause_service="frontend" if state != "HEALTHY" else None,
        decisions=[{"service": "frontend", "outcome": "SCALE_UP",
                    "current_replicas": 2, "delta": 1,
                    "target_replicas": 3, "reason": "test"}]
                  if state != "HEALTHY" else [],
        rejected=[],
        reasoning="test reasoning",
        cycle_duration_ms=2.5,
    )


def make_report(applied=1) -> ActuatorReport:
    report = ActuatorReport()
    for i in range(applied):
        report.results.append(ActuationResult(
            service="frontend",
            status=ActuationStatus.APPLIED,
            previous_replicas=2,
            target_replicas=3,
        ))
    return report


def test_writes_valid_json_lines():
    buf = io.StringIO()
    dl = DecisionLogger.__new__(DecisionLogger)
    dl._fh = buf

    record = dl.log(make_audit("UPSTREAM_LOAD_PRESSURE"), make_report(), cycle_number=1)

    buf.seek(0)
    line = buf.getvalue().strip()
    parsed = json.loads(line)

    assert parsed["cycle_number"] == 1
    assert parsed["pipeline_state"] == "UPSTREAM_LOAD_PRESSURE"
    assert parsed["root_cause_service"] == "frontend"
    assert "timestamp" in parsed
    assert "reasoning" in parsed
    assert "actuation_summary" in parsed
    print("PASS: test_writes_valid_json_lines")


def test_one_line_per_cycle():
    buf = io.StringIO()
    dl = DecisionLogger.__new__(DecisionLogger)
    dl._fh = buf

    for i in range(3):
        dl.log(make_audit(), make_report(0), cycle_number=i+1)

    buf.seek(0)
    lines = [l for l in buf.getvalue().strip().split("\n") if l]
    assert len(lines) == 3, f"Expected 3 lines, got {len(lines)}"
    for line in lines:
        json.loads(line)  # each line must be valid JSON
    print("PASS: test_one_line_per_cycle")


def test_actuation_summary_counts():
    report = ActuatorReport()
    report.results.append(ActuationResult("frontend", ActuationStatus.APPLIED, 2, 3))
    report.results.append(ActuationResult("cartservice", ActuationStatus.SKIPPED, 1, 1))
    report.results.append(ActuationResult("checkoutservice", ActuationStatus.FAILED, 1, 2, "timeout"))

    buf = io.StringIO()
    dl = DecisionLogger.__new__(DecisionLogger)
    dl._fh = buf

    record = dl.log(make_audit("UPSTREAM_LOAD_PRESSURE"), report, cycle_number=1)

    summary = record["actuation_summary"]
    assert summary["applied"] == 1
    assert summary["failed"] == 1
    assert summary["skipped"] == 1
    print("PASS: test_actuation_summary_counts")


def test_healthy_cycle_produces_empty_decisions():
    buf = io.StringIO()
    dl = DecisionLogger.__new__(DecisionLogger)
    dl._fh = buf

    record = dl.log(make_audit("HEALTHY"), make_report(0), cycle_number=5)

    assert record["pipeline_state"] == "HEALTHY"
    assert record["decisions"] == []
    assert record["root_cause_service"] is None
    print("PASS: test_healthy_cycle_produces_empty_decisions")


if __name__ == "__main__":
    tests = [
        test_writes_valid_json_lines,
        test_one_line_per_cycle,
        test_actuation_summary_counts,
        test_healthy_cycle_produces_empty_decisions,
    ]
    failed = 0
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\n{'All' if not failed else str(len(tests)-failed)}/{len(tests)} Decision Logger tests passed.")
    sys.exit(failed)
