"""Prometheus metrics for the FastAPI scoring service.

Exposed at GET /metrics in Prometheus text-exposition format -- scraped by
a Prometheus server (see docker-compose.yml's prometheus service, and
docs/observability.md for why that service could not be run end-to-end in
this sandbox, same Docker limitation as Kafka since Phase 2).

Metrics chosen map directly to the Phase 0 observability requirements for
the API layer: request count, latency, error rate, throughput -- plus
domain-specific ones (fraud alerts, risk-level distribution) that a
generic HTTP instrumentation library wouldn't know to add.
"""
from __future__ import annotations

from prometheus_client import Counter, Histogram

# --- generic HTTP metrics (also covered by prometheus-fastapi-instrumentator,
# see main.py -- these domain-specific ones below are what that library
# can't infer on its own) ---

SCORING_REQUESTS_TOTAL = Counter(
    "finguard_scoring_requests_total",
    "Total number of /score-transaction requests processed",
)

SCORING_ERRORS_TOTAL = Counter(
    "finguard_scoring_errors_total",
    "Total number of /score-transaction requests that errored",
    labelnames=("error_type",),
)

SCORING_LATENCY_SECONDS = Histogram(
    "finguard_scoring_latency_seconds",
    "Time to score a single transaction, including feature computation and model inference",
)

RISK_LEVEL_TOTAL = Counter(
    "finguard_risk_level_total",
    "Count of scored transactions by resulting risk level",
    labelnames=("risk_level",),
)

CUSTOMER_RISK_LOOKUPS_TOTAL = Counter(
    "finguard_customer_risk_lookups_total",
    "Total number of GET /customers/{id}/risk requests",
    labelnames=("found",),
)

ALERTS_LISTED_TOTAL = Counter(
    "finguard_alerts_listed_total",
    "Total number of alerts returned across all GET /alerts requests",
)
