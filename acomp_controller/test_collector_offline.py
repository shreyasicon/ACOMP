"""
test_collector_offline.py

Offline unit test for the Collector's internal parsing logic, using mocked
Prometheus HTTP responses instead of a live cluster connection. This
validates the pod-name-to-service matching, scalar extraction, and
aggregation logic in isolation -- run this anytime without needing
kubectl port-forward or cluster access.

Run with: python test_collector_offline.py
"""

from unittest.mock import MagicMock, patch

from acomp.collector import Collector, ServiceMetrics


def make_prom_response(result_list):
    """Builds a fake Prometheus /api/v1/query JSON response body."""
    return {"status": "success", "data": {"resultType": "vector", "result": result_list}}


def test_cpu_utilisation_aggregation():
    """Two pods belonging to 'cartservice' should be averaged into one
    cpu_utilisation figure under the service name 'cartservice'."""
    collector = Collector(
        prometheus_url="http://fake-prometheus:9090",
        namespace="default",
        service_names=["frontend", "cartservice", "checkoutservice"],
    )

    fake_results = [
        {"metric": {"pod": "cartservice-85df557f54-5vrcj"}, "value": [1234567890, "0.40"]},
        {"metric": {"pod": "cartservice-85df557f54-9zxqp"}, "value": [1234567890, "0.60"]},
        {"metric": {"pod": "frontend-794d665f96-2mfq9"}, "value": [1234567890, "0.20"]},
    ]

    mock_resp = MagicMock()
    mock_resp.json.return_value = make_prom_response(fake_results)
    mock_resp.raise_for_status.return_value = None

    with patch.object(collector._session, "get", return_value=mock_resp):
        result = collector._query_cpu_utilisation_all()

    assert abs(result["cartservice"] - 0.50) < 1e-9, f"Expected avg 0.50, got {result['cartservice']}"
    assert abs(result["frontend"] - 0.20) < 1e-9
    assert "checkoutservice" not in result  # no data for it in this fake response
    print("PASS: test_cpu_utilisation_aggregation")


def test_pod_name_longest_match():
    """A pod named 'cartservice-v2-xyz' should not be mis-attributed to a
    hypothetical shorter service name 'cart' if both existed; longest-prefix
    match must win."""
    collector = Collector(
        prometheus_url="http://fake-prometheus:9090",
        namespace="default",
        service_names=["cart", "cartservice"],
    )
    matched = collector._match_service_from_pod_name("cartservice-85df557f54-5vrcj")
    assert matched == "cartservice", f"Expected 'cartservice', got '{matched}'"
    print("PASS: test_pod_name_longest_match")


def test_replica_count_query():
    """kube_deployment_status_replicas_ready results should map deployment
    label directly onto service name, ignoring deployments not in our
    known service_names list (e.g. unrelated cluster deployments)."""
    collector = Collector(
        prometheus_url="http://fake-prometheus:9090",
        namespace="default",
        service_names=["frontend", "cartservice"],
    )

    fake_results = [
        {"metric": {"deployment": "frontend"}, "value": [1234567890, "3"]},
        {"metric": {"deployment": "cartservice"}, "value": [1234567890, "2"]},
        {"metric": {"deployment": "some-unrelated-deployment"}, "value": [1234567890, "5"]},
    ]

    mock_resp = MagicMock()
    mock_resp.json.return_value = make_prom_response(fake_results)
    mock_resp.raise_for_status.return_value = None

    with patch.object(collector._session, "get", return_value=mock_resp):
        result = collector._query_replica_counts_all()

    assert result == {"frontend": 3, "cartservice": 2}, f"Got {result}"
    print("PASS: test_replica_count_query")


def test_error_rate_handles_missing_data():
    """When Locust is not running, the error_rate query should return None
    rather than raising or crashing the poll cycle."""
    collector = Collector(
        prometheus_url="http://fake-prometheus:9090",
        namespace="default",
        service_names=["frontend"],
    )

    mock_resp = MagicMock()
    mock_resp.json.return_value = make_prom_response([])  # empty -- no Locust data
    mock_resp.raise_for_status.return_value = None

    with patch.object(collector._session, "get", return_value=mock_resp):
        result = collector._query_error_rate_entry_point()

    assert result is None, f"Expected None for empty result set, got {result}"
    print("PASS: test_error_rate_handles_missing_data")


def test_full_poll_cycle_with_mocks():
    """End-to-end poll() test: mocks every underlying query and checks the
    final MetricSnapshot is assembled correctly, including the entry-point
    service receiving its extra three metrics and other services not."""
    collector = Collector(
        prometheus_url="http://fake-prometheus:9090",
        namespace="default",
        service_names=["frontend", "cartservice"],
        entry_point_service="frontend",
    )

    # Patch each internal query method directly rather than mocking HTTP,
    # since poll() orchestrates multiple distinct queries.
    collector._query_cpu_utilisation_all = lambda: {"frontend": 0.55, "cartservice": 0.30}
    collector._query_replica_counts_all = lambda: {"frontend": 4, "cartservice": 2}
    collector._query_request_rate_entry_point = lambda: 120.5
    collector._query_latency_p99_entry_point = lambda: 245.0
    collector._query_error_rate_entry_point = lambda: 0.01

    snapshot = collector.poll()

    frontend = snapshot.get("frontend")
    cart = snapshot.get("cartservice")

    assert frontend.cpu_utilisation == 0.55
    assert frontend.replica_count == 4
    assert frontend.request_rate == 120.5
    assert frontend.latency_p99_ms == 245.0
    assert frontend.error_rate == 0.01

    assert cart.cpu_utilisation == 0.30
    assert cart.replica_count == 2
    assert cart.request_rate is None  # not the entry point -- correctly unset
    assert cart.latency_p99_ms is None
    assert cart.error_rate is None

    print("PASS: test_full_poll_cycle_with_mocks")


if __name__ == "__main__":
    test_cpu_utilisation_aggregation()
    test_pod_name_longest_match()
    test_replica_count_query()
    test_error_rate_handles_missing_data()
    test_full_poll_cycle_with_mocks()
    print("\nAll offline Collector tests passed.")
