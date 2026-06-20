"""
locust/locustfile.py

ACOMP load generator for Google Online Boutique. Simulates realistic shopper
behaviour against the frontend service (browse, add to cart, checkout) and
exposes Prometheus-compatible metrics directly via prometheus_client, so the
ACOMP Collector can read request rate, p99 latency, and error rate without
needing a separate exporter sidecar.

Exposed metrics (scraped by Prometheus on port 9646):
    acomp_locust_requests_total{status="success|failure"}   Counter
    acomp_locust_response_time_seconds                      Histogram
    acomp_locust_active_users                                Gauge

These names are deliberately prefixed with acomp_ to avoid clashing with any
other locust_-prefixed metrics that might exist in the cluster, and the
acomp.collector module's PromQL queries are written to match these exact
names (see collector.py: _query_request_rate_entry_point,
_query_latency_p99_entry_point, _query_error_rate_entry_point).

Task weights approximate a realistic shopper funnel: most traffic is
browsing (index, product views), a smaller fraction adds to cart, and a
smaller fraction still completes checkout -- mirroring the funnel shape
used in Google's own Online Boutique loadgenerator.
"""

import random

from locust import HttpUser, between, events, task
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ----------------------------------------------------------------------
# Prometheus metric definitions
# ----------------------------------------------------------------------

REQUESTS_TOTAL = Counter(
    "acomp_locust_requests_total",
    "Total requests issued by the ACOMP load generator",
    ["status"],
)

RESPONSE_TIME_SECONDS = Histogram(
    "acomp_locust_response_time_seconds",
    "Response time of requests issued by the ACOMP load generator",
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

ACTIVE_USERS = Gauge(
    "acomp_locust_active_users",
    "Current number of simulated concurrent users",
)

# Product IDs from Online Boutique's hard-coded catalogue, used to drive
# realistic add-to-cart and product-detail requests.
PRODUCT_IDS = [
    "0PUK6V6EV0", "1YMWWN1N4O", "2ZYFJ3GM2N", "66VCHSJNUP",
    "6E92ZMYYFZ", "9SIQT8TOJO", "L9ECAV7KIM", "LS4PSXUNUM", "OLJCESPC7Z",
]


# ----------------------------------------------------------------------
# Event hooks: wire Locust's own stats into the Prometheus metrics above
# ----------------------------------------------------------------------

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Starts the Prometheus HTTP metrics server once when Locust boots,
    on port 9646. This runs once per Locust process (master or standalone),
    not per simulated user."""
    start_http_server(9646)


@events.request.add_listener
def on_request(request_type, name, response_time, response_length,
                exception, context, **kwargs):
    """Fires on every single request Locust makes, success or failure.
    response_time is in milliseconds (Locust's native unit); converted to
    seconds for the Prometheus histogram, matching standard Prometheus
    convention of base-unit-seconds for time metrics."""
    if exception is not None:
        REQUESTS_TOTAL.labels(status="failure").inc()
    else:
        REQUESTS_TOTAL.labels(status="success").inc()
        RESPONSE_TIME_SECONDS.observe(response_time / 1000.0)


@events.spawning_complete.add_listener
def on_spawning_complete(user_count, **kwargs):
    """Updates the active user gauge whenever Locust finishes spawning
    (or removing) users to reach a new target concurrency level."""
    ACTIVE_USERS.set(user_count)


# ----------------------------------------------------------------------
# Shopper behaviour
# ----------------------------------------------------------------------

class OnlineBoutiqueShopper(HttpUser):
    """
    Simulates a single shopper session against the Online Boutique frontend.
    Task weights bias toward browsing, consistent with a typical retail
    funnel where most visits do not convert to a purchase.
    """
    wait_time = between(1, 5)

    def on_start(self):
        """Each simulated user sets an initial currency preference, mirroring
        real shopper behaviour on first visit."""
        self.client.get("/", name="/ (home)")

    @task(10)
    def browse_home(self):
        self.client.get("/", name="/ (home)")

    @task(8)
    def view_product(self):
        product_id = random.choice(PRODUCT_IDS)
        self.client.get(f"/product/{product_id}", name="/product/[id]")

    @task(3)
    def add_to_cart(self):
        product_id = random.choice(PRODUCT_IDS)
        self.client.post(
            f"/cart",
            data={"product_id": product_id, "quantity": random.randint(1, 3)},
            name="/cart (add)",
        )

    @task(2)
    def view_cart(self):
        self.client.get("/cart", name="/cart (view)")

    @task(1)
    def checkout(self):
        """Completes a full checkout using Online Boutique's hard-coded
        test payment details, matching the values used in Google's own
        sample load generator."""
        self.client.post(
            "/cart/checkout",
            data={
                "email": "acomp-loadtest@example.com",
                "street_address": "1600 Amphitheatre Parkway",
                "zip_code": "94043",
                "city": "Mountain View",
                "state": "CA",
                "country": "United States",
                "credit_card_number": "4432-8015-6152-0454",
                "credit_card_expiration_month": "1",
                "credit_card_expiration_year": "2030",
                "credit_card_cvv": "672",
            },
            name="/cart/checkout",
        )
