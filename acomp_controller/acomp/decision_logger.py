"""
acomp/decision_logger.py

The Decision Logger component of ACOMP. Receives the full decision context
from the Policy Engine and Actuator each cycle and writes a structured
JSON Lines record to stdout (captured by Kubernetes logging infrastructure
and forwarded to Azure Monitor Logs / Log Analytics).

Per the thesis component specification table:
    Input:      AuditRecord from Policy Engine + ActuatorReport from Actuator
    Processing: Merges actuation outcomes into the audit record, serialises
                to JSON, writes one line per cycle
    Output:     JSON Lines record per cycle to stdout

JSON Lines format (one record per line, newline-delimited):
    {
      "timestamp":        "2026-06-21T10:00:12.960273+00:00",
      "cycle_number":     42,
      "pipeline_state":   "UPSTREAM_LOAD_PRESSURE",
      "root_cause":       "frontend",
      "decisions":        [...],
      "rejected":         [...],
      "actuation":        [...],
      "reasoning":        "frontend: CPU=82.1% ...",
      "cycle_duration_ms": 3.14
    }

This format is directly queryable in Azure Monitor Log Analytics via KQL
(Kusto Query Language), enabling operators to filter, aggregate, and audit
every decision without accessing the controller code or understanding the
underlying algorithm. This satisfies SQ4 (operational explainability).
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from .policy_engine import AuditRecord
from .actuator import ActuatorReport

logger = logging.getLogger("acomp.decision_logger")


class DecisionLogger:
    """
    Writes one JSON Lines record per ACOMP control cycle.

    Usage:
        decision_logger = DecisionLogger()
        decision_logger.log(audit_record, actuator_report, cycle_number=1)

    Output goes to stdout by default so Kubernetes captures it via its
    standard container log collection pipeline and forwards it to whatever
    log aggregation backend is configured (Azure Monitor Logs in this study).
    An optional file path can be specified for local testing.
    """

    def __init__(self, output_file: Optional[str] = None):
        """
        output_file: if None, writes to stdout (default, for production).
                     If a file path string, writes to that file (for testing).
        """
        if output_file is not None:
            self._fh = open(output_file, "a", encoding="utf-8")
            logger.info("Decision Logger: writing to file %s", output_file)
        else:
            self._fh = sys.stdout
            logger.info("Decision Logger: writing to stdout")

    def log(
        self,
        audit: AuditRecord,
        actuation: ActuatorReport,
        cycle_number: int = 0,
    ) -> dict:
        """
        Merges the AuditRecord from the Policy Engine with the ActuatorReport
        from the Actuator and writes a single JSON Lines record.

        Returns the record dict (useful for testing without needing to parse
        the written output back from the file/stdout).
        """
        record = {
            "timestamp": audit.timestamp,
            "cycle_number": cycle_number,
            "pipeline_state": audit.pipeline_state,
            "root_cause_service": audit.root_cause_service,
            "decisions": audit.decisions,
            "rejected": audit.rejected,
            "actuation": actuation.to_list(),
            "reasoning": audit.reasoning,
            "cycle_duration_ms": audit.cycle_duration_ms,
            "actuation_summary": {
                "applied": len(actuation.applied()),
                "failed": len(actuation.failed()),
                "skipped": len(actuation.results) - len(actuation.applied()) - len(actuation.failed()),
            },
        }

        line = json.dumps(record, ensure_ascii=False)
        print(line, file=self._fh, flush=True)

        logger.debug(
            "Cycle %d logged: state=%s root=%s applied=%d rejected=%d",
            cycle_number,
            audit.pipeline_state,
            audit.root_cause_service,
            len(actuation.applied()),
            len(audit.rejected),
        )

        return record

    def close(self) -> None:
        """Closes the output file if one was opened. No-op for stdout."""
        if self._fh is not sys.stdout:
            self._fh.close()
