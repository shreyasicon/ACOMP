"""
acomp/collector.py

The Collector component of ACOMP. Polls Prometheus every 15 seconds and
normalises raw time-series data into a consistent MetricSnapshot per service,
ready for consumption by the Policy Engine.

Per Table "ACOMP component specification" in the thesis:
    Input:      Prometheus HTTP API every 15s
    Processing: Normalises CPU, p99 latency, error rate, request rate per service
    Output:     Consistent metric snapshot for all pipeline services

Metric sourcing strategy (practical adaptation, see thesis Section 5.1):
    - CPU per service:        cAdvisor container_cpu_usage_seconds_total,
                               scraped automatically by kube-prometheus-stack.
    - Replica count:          kube_deployment_status_replicas via kube-state-metrics.
    - Request rate / p99
      latency / error rate:   measured at the pipeline entry point (frontend)
                               via the ACOMP locustfile's native Prometheus
                               export (see locust/locustfile.py), since
                               Online Boutique does not natively expose
                               per-service application metrics to Prometheus.
                               This is consistent with SQ3, which evaluates
                               pipeline-level latency and SLO compliance rather
                               than requiring per-hop tracing.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

logger = logging.getLogger("acomp.collector")

# How far back each instant query looks when computing rates, e.g. for
# rate(container_cpu_usage_seconds_total[1m]). 1m is standard for a 15s
# scrape interval -- it smooths over four scrape points.
RATE_WINDOW = "1m"

# Default control cycle interval. Matches the 15s figure used throughout
# the thesis (Methodology Section, Algorithm 1 docstring, architecture
# diagram label "15s poll").
DEFAULT_POLL_INTERVAL_SECONDS = 15.0


@dataclass
class ServiceMetrics:
    """Normalised metric snapshot for a single service at one point in time.

    cpu_utilisation:   fraction of requested CPU actually used, e.g. 0.82 for 82%.
                        None if the service has no CPU request set or no data yet.
    replica_count:      current number of Ready replicas for the Deployment.
    request_rate:       requests/sec. Only populated for the entry-point service
                         (frontend) where Locust measures it directly; None for
                         internal services under the cAdvisor-only strategy.
    latency_p99_ms:      p99 response latency in milliseconds. Same entry-point
                         caveat as request_rate.
    error_rate:         fraction of requests that returned an error (0.0-1.0).
                         Same entry-point caveat as request_rate.
    """
    name: str
    cpu_utilisation: float | None = None
    replica_count: int | None = None
    request_rate: float | None = None
    latency_p99_ms: float | None = None
    error_rate: float | None = None


@dataclass
class MetricSnapshot:
    """The full per-cycle output of the Collector: one ServiceMetrics per
    known pipeline service, plus the timestamp the snapshot was taken at."""
    timestamp: datetime
    services: dict[str, ServiceMetrics] = field(default_factory=dict)

    def get(self, service_name: str) -> ServiceMetrics | None:
        return self.services.get(service_name)


class PrometheusQueryError(RuntimeError):
    """Raised when a Prometheus HTTP API query fails or returns malformed data."""


class Collector:
    """
    Polls a Prometheus instance and produces MetricSnapshot objects.

    Usage:
        collector = Collector(
            prometheus_url="http://prometheus-kube-prometheus-prometheus.monitoring:9090",
            namespace="default",
            service_names=context_map.service_names(),
            entry_point_service="frontend",
        )
        snapshot = collector.poll()
    """

    def __init__(
        self,
        prometheus_url: str,
        namespace: str,
        service_names: list[str],
        entry_point_service: str = "frontend",
        request_timeout_seconds: float = 10.0,
    ):
        self.prometheus_url = prometheus_url.rstrip("/")
        self.namespace = namespace
        self.service_names = service_names
        self.entry_point_service = entry_point_service
        self.request_timeout_seconds = request_timeout_seconds
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll(self) -> MetricSnapshot:
        """Performs one full collection cycle: queries Prometheus for CPU
        and replica count for every known service, and pulls request rate /
        latency / error rate for the entry-point service only. Returns a
        complete MetricSnapshot. Individual query failures are logged and
        leave the corresponding field as None rather than raising, so that
        one missing metric does not abort the whole cycle -- the Policy
        Engine is responsible for deciding how to handle incomplete data."""
        timestamp = datetime.now(timezone.utc)
        snapshot = MetricSnapshot(timestamp=timestamp)

        cpu_by_service = self._query_cpu_utilisation_all()
        replicas_by_service = self._query_replica_counts_all()

        for name in self.service_names:
            metrics = ServiceMetrics(
                name=name,
                cpu_utilisation=cpu_by_service.get(name),
                replica_count=replicas_by_service.get(name),
            )
            snapshot.services[name] = metrics

        # Entry-point-only metrics (request rate, p99 latency, error rate)
        entry = snapshot.services.get(self.entry_point_service)
        if entry is not None:
            entry.request_rate = self._query_request_rate_entry_point()
            entry.latency_p99_ms = self._query_latency_p99_entry_point()
            entry.error_rate = self._query_error_rate_entry_point()
        else:
            logger.warning(
                "Entry-point service '%s' not found in service_names; "
                "skipping request rate / latency / error rate collection",
                self.entry_point_service,
            )

        logger.debug("Collector poll complete: %s", snapshot)
        return snapshot

    def run_forever(self, on_snapshot, interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS):
        """Polls in a loop every interval_seconds, calling on_snapshot(snapshot)
        after each successful poll. Drift-corrects the sleep so the average
        cadence stays close to interval_seconds even if a poll takes time."""
        logger.info("Collector starting poll loop, interval=%.1fs", interval_seconds)
        while True:
            cycle_start = time.monotonic()
            try:
                snapshot = self.poll()
                on_snapshot(snapshot)
            except Exception:
                logger.exception("Collector poll cycle failed; will retry next interval")

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.0, interval_seconds - elapsed)
            time.sleep(sleep_for)

    # ------------------------------------------------------------------
    # Internal Prometheus query helpers
    # ------------------------------------------------------------------

    def _instant_query(self, promql: str) -> list[dict]:
        """Executes a Prometheus instant query and returns the raw 'result'
        list from the response. Raises PrometheusQueryError on HTTP failure
        or a non-'success' API status."""
        url = f"{self.prometheus_url}/api/v1/query"
        try:
            resp = self._session.get(
                url, params={"query": promql}, timeout=self.request_timeout_seconds
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise PrometheusQueryError(f"HTTP request failed for query '{promql}': {exc}") from exc

        body = resp.json()
        if body.get("status") != "success":
            raise PrometheusQueryError(f"Prometheus returned non-success status: {body}")

        return body["data"]["result"]

    def _safe_instant_query(self, promql: str, context: str) -> list[dict]:
        """Wraps _instant_query, logging and returning [] on failure instead
        of propagating, so that a single bad query doesn't crash the poll."""
        try:
            return self._instant_query(promql)
        except PrometheusQueryError as exc:
            logger.warning("Query failed (%s): %s", context, exc)
            return []

    def _query_cpu_utilisation_all(self) -> dict[str, float]:
        """Returns {service_name: cpu_utilisation_fraction} for every service
        with available cAdvisor data, computed as:

            rate(container_cpu_usage_seconds_total[1m])
                / on(pod) kube_pod_container_resource_requests{resource="cpu"}

        i.e. actual CPU seconds consumed per second, divided by the CPU
        request, matching the same utilisation definition the native
        Kubernetes HPA uses (see Equation 5 in the thesis)."""
        promql = (
            f'sum by (pod) ('
            f'  rate(container_cpu_usage_seconds_total{{namespace="{self.namespace}", '
            f'  container!="", container!="POD"}}[{RATE_WINDOW}])'
            f') / on(pod) '
            f'sum by (pod) ('
            f'  kube_pod_container_resource_requests{{namespace="{self.namespace}", resource="cpu"}}'
            f')'
        )
        results = self._safe_instant_query(promql, "cpu_utilisation")
        return self._aggregate_by_service_label(results, label="pod")

    def _query_replica_counts_all(self) -> dict[str, int]:
        """Returns {service_name: ready_replica_count} via kube-state-metrics."""
        promql = (
            f'kube_deployment_status_replicas_ready{{namespace="{self.namespace}"}}'
        )
        results = self._safe_instant_query(promql, "replica_count")
        out: dict[str, int] = {}
        for r in results:
            deployment = r["metric"].get("deployment")
            value = r["value"][1]
            if deployment and deployment in self.service_names:
                try:
                    out[deployment] = int(float(value))
                except (TypeError, ValueError):
                    continue
        return out

    def _query_request_rate_entry_point(self) -> float | None:
        """Requests/sec at the frontend, as measured by the ACOMP locustfile's
        exported acomp_locust_requests_total counter (see locust/locustfile.py).
        Sums both success and failure labels, since request rate should count
        every attempt regardless of outcome. Returns None if Locust is not
        currently running or the metric is unavailable (e.g. between
        evaluation scenarios)."""
        promql = f'sum(rate(acomp_locust_requests_total[{RATE_WINDOW}]))'
        results = self._safe_instant_query(promql, "request_rate")
        return self._first_scalar(results)

    def _query_latency_p99_entry_point(self) -> float | None:
        """p99 response latency in milliseconds, from the ACOMP locustfile's
        acomp_locust_response_time_seconds histogram (see locust/locustfile.py).
        Returns None if unavailable."""
        promql = (
            f'histogram_quantile(0.99, sum(rate('
            f'acomp_locust_response_time_seconds_bucket[{RATE_WINDOW}])) by (le)) * 1000'
        )
        results = self._safe_instant_query(promql, "latency_p99")
        return self._first_scalar(results)

    def _query_error_rate_entry_point(self) -> float | None:
        """Fraction of requests that failed, computed from the ACOMP
        locustfile's acomp_locust_requests_total counter split by the
        status="failure"/"success" label (see locust/locustfile.py).
        Returns None if unavailable or if there have been zero requests in
        the window (avoids a spurious 0/0)."""
        promql = (
            f'(sum(rate(acomp_locust_requests_total{{status="failure"}}[{RATE_WINDOW}])) '
            f'or vector(0)) '
            f'/ sum(rate(acomp_locust_requests_total[{RATE_WINDOW}]))'
        )
        results = self._safe_instant_query(promql, "error_rate")
        return self._first_scalar(results)

    # ------------------------------------------------------------------
    # Result-parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _first_scalar(results: list[dict]) -> float | None:
        """Extracts the value from the first (and expected only) result of
        a scalar/vector query. Returns None if results is empty or the
        value cannot be parsed as a float."""
        if not results:
            return None
        try:
            return float(results[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def _aggregate_by_service_label(
        self, results: list[dict], label: str
    ) -> dict[str, float]:
        """Maps raw per-pod Prometheus results onto service names by matching
        the pod name prefix against known service_names. Kubernetes pod names
        follow the pattern '<deployment-name>-<replicaset-hash>-<pod-hash>',
        so the service name is recovered by checking which known service name
        the pod name starts with. Where multiple pods belong to the same
        service, their values are averaged, which is appropriate for a
        utilisation fraction (CPU% is meaningful averaged across replicas)."""
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}

        for r in results:
            pod_name = r["metric"].get(label, "")
            value = r["value"][1]
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue

            matched_service = self._match_service_from_pod_name(pod_name)
            if matched_service is None:
                continue

            sums[matched_service] = sums.get(matched_service, 0.0) + value
            counts[matched_service] = counts.get(matched_service, 0) + 1

        return {svc: sums[svc] / counts[svc] for svc in sums}

    def _match_service_from_pod_name(self, pod_name: str) -> str | None:
        """Finds the longest known service_name that is a prefix of pod_name
        followed by a '-'. Using longest-match avoids 'cart' incorrectly
        matching a pod actually belonging to 'cartservice-v2' style names."""
        best_match: str | None = None
        for svc in self.service_names:
            if pod_name.startswith(svc + "-") or pod_name == svc:
                if best_match is None or len(svc) > len(best_match):
                    best_match = svc
        return best_match
