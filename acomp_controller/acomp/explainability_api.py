"""
acomp/explainability_api.py

Live REST explainability API for ACOMP.

Serves recent audit records and root cause summaries over HTTP so any
operator can inspect why the last N scaling decisions were made without
accessing pod logs or Azure Monitor.

Endpoints:
    GET /decisions?last=10          last N audit records (default 10)
    GET /decisions/latest           single most recent record
    GET /summary                    root cause frequency + SLO trend
    GET /health                     controller liveness
    GET /metrics                    Prometheus-compatible text metrics

Usage (started in a background thread from main.py):
    from acomp.explainability_api import ExplainabilityAPI
    api = ExplainabilityAPI(decision_logger, port=8080)
    api.start()          # non-blocking, runs in daemon thread
    ...
    api.stop()
"""

from __future__ import annotations

import collections
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

if TYPE_CHECKING:
    from .decision_logger import DecisionLogger

logger = logging.getLogger("acomp.api")


class ExplainabilityAPI:
    """
    Lightweight HTTP server exposing ACOMP decision audit records.

    All state is shared from the DecisionLogger's in-memory ring buffer.
    No additional storage required — reads the same records that go to
    stdout/Azure Monitor.
    """

    def __init__(self, decision_logger: "DecisionLogger", port: int = 8080):
        self._dl = decision_logger
        self._port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self):
        """Start the API server in a daemon background thread."""
        api_ref = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass  # suppress access logs from HTTP server

            def do_GET(self):
                parsed = urlparse(self.path)
                path = parsed.path.rstrip("/")
                params = parse_qs(parsed.query)

                try:
                    if path == "/decisions":
                        n = int(params.get("last", ["10"])[0])
                        body = api_ref._handle_decisions(n)
                    elif path == "/decisions/latest":
                        body = api_ref._handle_latest()
                    elif path == "/summary":
                        body = api_ref._handle_summary()
                    elif path == "/health":
                        body = api_ref._handle_health()
                    elif path == "/metrics":
                        body = api_ref._handle_prometheus_metrics()
                        self._send(200, body, "text/plain; version=0.0.4")
                        return
                    else:
                        self._send(404, json.dumps({"error": "not found"}))
                        return
                    self._send(200, body)
                except Exception as e:
                    self._send(500, json.dumps({"error": str(e)}))

            def _send(self, code, body, ct="application/json"):
                b = body.encode()
                self.send_response(code)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", len(b))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(b)

        try:
            self._server = HTTPServer(("0.0.0.0", self._port), Handler)
            self._thread = threading.Thread(
                target=self._server.serve_forever, daemon=True
            )
            self._thread.start()
            logger.info("Explainability API started on port %d", self._port)
            logger.info(
                "  GET /decisions?last=10  — recent audit records\n"
                "  GET /summary            — root cause frequency + SLO trend\n"
                "  GET /health             — liveness\n"
                "  GET /metrics            — Prometheus metrics"
            )
        except Exception as e:
            logger.warning("Explainability API failed to start: %s", e)

    def stop(self):
        if self._server:
            self._server.shutdown()

    # ── Handlers ─────────────────────────────────────────────────────

    def _handle_decisions(self, n: int) -> str:
        records = list(self._dl.recent(min(n, 100)))
        return json.dumps({
            "count": len(records),
            "records": records,
        }, indent=2, ensure_ascii=False)

    def _handle_latest(self) -> str:
        records = list(self._dl.recent(1))
        if not records:
            return json.dumps({"error": "no records yet"})
        r = records[-1]
        # Build human-readable explanation
        explanation = _explain(r)
        return json.dumps({
            "record": r,
            "explanation": explanation,
        }, indent=2, ensure_ascii=False)

    def _handle_summary(self) -> str:
        records = list(self._dl.recent(200))
        if not records:
            return json.dumps({"message": "no records yet"})

        # Root cause frequency
        root_causes: dict[str, int] = collections.Counter()
        states: dict[str, int] = collections.Counter()
        total_applied = 0
        total_skipped = 0
        slo_violations = 0
        p99_trend: list[float] = []

        for r in records:
            state = r.get("pipeline_state", "UNKNOWN")
            states[state] += 1
            rc = r.get("root_cause_service")
            if rc:
                root_causes[rc] += 1
            a = r.get("actuation_summary", {})
            total_applied += a.get("applied", 0)
            total_skipped += a.get("skipped", 0)
            # SLO trend from decisions list
            for d in r.get("decisions", []):
                if "p99" in str(r.get("reasoning", "")):
                    pass
            p99 = r.get("p99_latency_ms")
            if p99 is not None:
                p99_trend.append(float(p99))
                if float(p99) > 500:
                    slo_violations += 1

        # SLO trend direction
        trend = "stable"
        if len(p99_trend) >= 3:
            recent_avg = sum(p99_trend[-3:]) / 3
            older_avg = sum(p99_trend[:3]) / 3 if len(p99_trend) >= 6 else recent_avg
            if recent_avg > older_avg * 1.1:
                trend = "degrading"
            elif recent_avg < older_avg * 0.9:
                trend = "improving"

        return json.dumps({
            "cycles_analysed": len(records),
            "pipeline_state_distribution": dict(states),
            "root_cause_frequency": dict(root_causes.most_common(5)),
            "top_root_cause": root_causes.most_common(1)[0][0] if root_causes else None,
            "total_scale_events_applied": total_applied,
            "total_patches_skipped": total_skipped,
            "slo_violations_detected": slo_violations,
            "p99_trend": trend,
            "p99_recent_avg_ms": round(sum(p99_trend[-5:]) / len(p99_trend[-5:]), 1) if p99_trend else None,
        }, indent=2)

    def _handle_health(self) -> str:
        records = list(self._dl.recent(1))
        last_ts = records[-1].get("timestamp") if records else None
        return json.dumps({
            "status": "ok",
            "total_cycles": self._dl.total_cycles,
            "last_cycle_at": last_ts,
            "uptime_cycles": self._dl.total_cycles,
        })

    def _handle_prometheus_metrics(self) -> str:
        """Prometheus text format metrics for scraping."""
        records = list(self._dl.recent(50))
        states: dict[str, int] = collections.Counter()
        total_applied = total_skipped = 0
        for r in records:
            states[r.get("pipeline_state", "UNKNOWN")] += 1
            a = r.get("actuation_summary", {})
            total_applied += a.get("applied", 0)
            total_skipped += a.get("skipped", 0)

        lines = [
            "# HELP acomp_cycles_total Total control loop cycles",
            "# TYPE acomp_cycles_total counter",
            f'acomp_cycles_total {self._dl.total_cycles}',
            "# HELP acomp_scale_events_applied Scale patches applied to Kubernetes",
            "# TYPE acomp_scale_events_applied counter",
            f'acomp_scale_events_applied {total_applied}',
            "# HELP acomp_scale_events_skipped Redundant patches skipped",
            "# TYPE acomp_scale_events_skipped counter",
            f'acomp_scale_events_skipped {total_skipped}',
        ]
        for state, count in states.items():
            lines.append(
                f'acomp_pipeline_state_cycles{{state="{state}"}} {count}'
            )
        return "\n".join(lines) + "\n"


def _explain(record: dict) -> str:
    """Generate a plain-English explanation of a single audit record."""
    state = record.get("pipeline_state", "UNKNOWN")
    root = record.get("root_cause_service", "unknown")
    reasoning = record.get("reasoning", "")
    decisions = record.get("decisions", [])
    actuation = record.get("actuation_summary", {})

    applied = actuation.get("applied", 0)
    skipped = actuation.get("skipped", 0)

    scaled = [d["service"] for d in decisions if d.get("outcome") == "SCALE_UP"]
    suppressed = [d["service"] for d in decisions if d.get("outcome") == "SUPPRESSED"]

    lines = [f"Pipeline state: {state}"]
    if state == "HEALTHY":
        lines.append("All services operating within normal parameters. No scaling required.")
    elif state == "UPSTREAM_LOAD_PRESSURE":
        lines.append(f"Root cause: {root} is under load pressure.")
        if scaled:
            lines.append(f"Scaled up: {', '.join(scaled)}.")
        if suppressed:
            lines.append(f"Suppressed (below propagation threshold): {', '.join(suppressed)}.")
    elif state == "DOWNSTREAM_DEGRADATION":
        lines.append(f"Downstream slowness detected at {root}.")
        lines.append("Scaling suppressed — adding replicas would not address root cause.")
    elif state == "PIPELINE_CEILING":
        lines.append("All services at maximum replica count. Cannot scale further.")

    lines.append(f"Actions: {applied} applied, {skipped} skipped.")
    return " ".join(lines)
