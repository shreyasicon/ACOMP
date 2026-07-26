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
    known pipeline service, plus the timestamp the snapshot was taken at.

    Pipeline-level metrics (request_rate, p99_latency_ms, error_rate) are
    taken from the entry-point service and stored here for convenience so
    main.py can access them without iterating over services.
    """
    timestamp: datetime
    services: dict[str, ServiceMetrics] = field(default_factory=dict)
    request_rate:   float | None = None   # entry-point req/s
    p99_latency_ms: float | None = None   # entry-point p99 latency
    error_rate:     float | None = None   # entry-point error rate
    slope_signal:   bool         = False  # True when req/s rising >15%/cycle for 2+ cycles
    rate_slope_pct: float        = 0.0   # current req/s % change vs previous cycle
    slo_trend_alert: bool        = False  # True when linear trend predicts SLO breach within 2 cycles
    predicted_p99_ms: float | None = None # predicted p99 2 cycles ahead via linear regression

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
        # ── EWMA smoothing state ────────────────────────────────────
        # Exponential weighted moving average for CPU readings (alpha=0.3)
        # Filters transient single-cycle spikes from triggering scaling
        self._ewma_cpu: dict[str, float] = {}
        self._ewma_alpha = 0.3
        # ── Slope detection state ───────────────────────────────────
        self._rate_history: list[float] = []
        self._consecutive_rising: int = 0
        self._slope_threshold = 0.15  # 15% rise per cycle
        # ── SLO trend prediction state ───────────────────────────────
        # 5-point linear regression on p99 readings predicts latency
        # 2 cycles ahead — pre-scales before SLO is actually breached
        self._p99_history: list[float] = []
        self._slo_threshold_ms = 500.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll(self) -> MetricSnapshot:
        """Performs one full collection cycle using parallel per-service polling.

        Each service is polled in its own thread simultaneously — matching
        Smart HPA's parallel process architecture that gave it faster spike
        detection in Scenario 1. With 11 services polled in parallel, the
        effective poll latency is the slowest single query (~0.3s) rather
        than the sum of all queries (~3s sequential).

        Individual query failures leave the field as None rather than
        raising — the Policy Engine handles incomplete data gracefully.
        """
        timestamp = datetime.now(timezone.utc)
        snapshot = MetricSnapshot(timestamp=timestamp)

        # ── Parallel per-service metrics collection ───────────────────
        # Each service gets its own thread — same architecture as Smart HPA's
        # multiprocessing.Pool(processes=len(functions)) call in their
        # Microservice Capacity Analyzer. This gives ACOMP the same parallel
        # detection speed advantage for traffic spikes.
        import concurrent.futures

        def collect_service(name: str) -> tuple[str, ServiceMetrics]:
            cpu = self._query_cpu_for_service(name)
            replicas = self._query_replicas_for_service(name)

            # Apply EWMA smoothing to CPU
            if cpu is not None:
                prev_ewma = self._ewma_cpu.get(name, cpu)
                cpu = self._ewma_alpha * cpu + (1 - self._ewma_alpha) * prev_ewma
                self._ewma_cpu[name] = cpu

            return name, ServiceMetrics(
                name=name,
                cpu_utilisation=cpu,
                replica_count=replicas,
            )

        # Run all service collections in parallel
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self.service_names),
            thread_name_prefix="acomp-collector"
        ) as executor:
            futures = {
                executor.submit(collect_service, name): name
                for name in self.service_names
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    name, metrics = future.result(timeout=self.request_timeout_seconds)
                    snapshot.services[name] = metrics
                except Exception as e:
                    name = futures[future]
                    logger.warning("Parallel poll failed for %s: %s", name, e)
                    snapshot.services[name] = ServiceMetrics(name=name)

        # ── Entry-point metrics (sequential — single service) ─────────
        entry = snapshot.services.get(self.entry_point_service)
        if entry is not None:
            entry.request_rate    = self._query_request_rate_entry_point()
            entry.latency_p99_ms  = self._query_latency_p99_entry_point()
            entry.error_rate      = self._query_error_rate_entry_point()
            snapshot.request_rate   = entry.request_rate
            snapshot.p99_latency_ms = entry.latency_p99_ms
            snapshot.error_rate     = entry.error_rate

            # ── Slope detection: 3-point rolling window ───────────────
            if entry.request_rate is not None:
                self._rate_history.append(entry.request_rate)
                if len(self._rate_history) > 3:
                    self._rate_history.pop(0)

                if len(self._rate_history) >= 2:
                    prev = self._rate_history[-2]
                    curr = self._rate_history[-1]
                    if prev > 0:
                        slope_pct = (curr - prev) / prev
                        snapshot.rate_slope_pct = round(slope_pct * 100, 1)
                        if slope_pct > self._slope_threshold:
                            self._consecutive_rising += 1
                        else:
                            self._consecutive_rising = 0
                        snapshot.slope_signal = self._consecutive_rising >= 2
                        if snapshot.slope_signal:
                            logger.info(
                                "Slope signal: req/s %.1f→%.1f (+%.0f%%) "
                                "for %d consecutive cycles -- pre-scale activated",
                                prev, curr, slope_pct * 100,
                                self._consecutive_rising
                            )
        else:
            logger.warning(
                "Entry-point service '%s' not found in service_names; "
                "skipping request rate / latency / error rate collection",
                self.entry_point_service,
            )

            # ── SLO trend prediction: 5-point linear regression ──────
            # Predict p99 two cycles ahead using linear regression.
            # If predicted p99 > 500ms, set slo_trend_alert=True so
            # Policy Engine can pre-scale before the breach occurs.
            # This is genuinely predictive — no other reviewed paper
            # does this deterministically without ML.
            if entry.latency_p99_ms is not None:
                self._p99_history.append(entry.latency_p99_ms)
                if len(self._p99_history) > 5:
                    self._p99_history.pop(0)

                if len(self._p99_history) >= 3:
                    n = len(self._p99_history)
                    x = list(range(n))
                    xm = sum(x) / n
                    ym = sum(self._p99_history) / n
                    num = sum((xi - xm) * (yi - ym)
                              for xi, yi in zip(x, self._p99_history))
                    den = sum((xi - xm) ** 2 for xi in x)
                    if den > 0:
                        slope = num / den
                        intercept = ym - slope * xm
                        # Predict 2 cycles ahead
                        predicted = slope * (n + 1) + intercept
                        snapshot.predicted_p99_ms = round(predicted, 1)
                        if predicted > self._slo_threshold_ms:
                            snapshot.slo_trend_alert = True
                            logger.info(
                                "SLO trend alert: predicted p99=%.0fms in 2 cycles "
                                "(current=%.0fms, slope=%.1f ms/cycle)",
                                predicted, entry.latency_p99_ms, slope
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
            f') / '
            f'sum by (pod) ('
            f'  kube_pod_container_resource_requests{{namespace="{self.namespace}", resource="cpu", container!=""}}'
            f')'
        )
        results = self._safe_instant_query(promql, "cpu_utilisation")
        return self._aggregate_by_service_label(results, label="pod")

    def _query_replicas_for_service(self, service_name: str) -> int | None:
        """Per-service replica count query — used by parallel polling threads."""
        promql = (
            f'kube_deployment_status_replicas_ready{{'
            f'namespace="{self.namespace}",deployment="{service_name}"}}'
        )
        results = self._safe_instant_query(promql, f"replicas_{service_name}")
        for r in results:
            try:
                return int(float(r["value"][1]))
            except (TypeError, ValueError, KeyError):
                continue
        return None

    def _query_cpu_for_service(self, service_name: str) -> float | None:
        """Per-service CPU utilisation — used by parallel polling threads.
        Each service gets its own independent Prometheus query in its own
        thread, matching Smart HPA's parallel Microservice Manager architecture.
        """
        promql = (
            f'sum('
            f'  rate(container_cpu_usage_seconds_total{{namespace="{self.namespace}",'
            f'  pod=~"{service_name}-.*",container!="",container!="POD"}}[{RATE_WINDOW}])'
            f') / sum('
            f'  kube_pod_container_resource_requests{{namespace="{self.namespace}",'
            f'  resource="cpu",container!="",pod=~"{service_name}-.*"}}'
            f')'
        )
        results = self._safe_instant_query(promql, f"cpu_{service_name}")
        for r in results:
            try:
                return float(r["value"][1])
            except (TypeError, ValueError, KeyError):
                continue
        return None

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
