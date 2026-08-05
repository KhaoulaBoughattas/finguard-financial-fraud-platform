"""Tests for the Prometheus /metrics endpoint.

Skipped automatically under the same conditions as test_api.py (Postgres
or the production model unavailable) -- these tests make real requests
through the app and assert real counter/histogram values change as a
result, not just that the endpoint returns 200.
"""
import pytest

from src.common.config import get_settings


def _dependencies_available() -> bool:
    try:
        import mlflow

        from src.api import repository
        from src.ml.mlflow_tracking import configure_mlflow, get_production_model_uri

        settings = get_settings()
        if not repository.check_database_health(settings.postgres):
            return False
        configure_mlflow(settings.ml)
        mlflow.pyfunc.load_model(get_production_model_uri("finguard-fraud-xgboost"))
        return True
    except Exception:  # noqa: BLE001 -- availability probe, same rationale as
        # every other infra-dependent test module in this project.
        return False


pytestmark = pytest.mark.skipif(
    not _dependencies_available(),
    reason="Postgres or the production MLflow model is not available",
)


@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient

    from src.api.main import app

    return TestClient(app)


@pytest.fixture(scope="module")
def real_ids(ensure_sufficient_data):
    import psycopg

    settings = get_settings().postgres
    conn = psycopg.connect(
        host=settings.host, port=settings.port, dbname=settings.db,
        user=settings.user, password=settings.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers LIMIT 1")
            customer_id = cur.fetchone()[0]
            cur.execute("SELECT account_id FROM accounts WHERE customer_id = %s LIMIT 1", (customer_id,))
            account_id = cur.fetchone()[0]
            cur.execute("SELECT merchant_id FROM merchants LIMIT 1")
            merchant_id = cur.fetchone()[0]
        return {"customer_id": customer_id, "account_id": account_id, "merchant_id": merchant_id}
    finally:
        conn.close()


def _get_counter_value(metrics_text: str, metric_line_prefix: str) -> float | None:
    for line in metrics_text.splitlines():
        if line.startswith(metric_line_prefix):
            return float(line.rsplit(" ", 1)[-1])
    return None


def test_metrics_endpoint_returns_prometheus_text_format(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "# HELP" in response.text
    assert "# TYPE" in response.text


def test_metrics_includes_generic_http_request_metrics(client):
    client.get("/health")
    response = client.get("/metrics")
    assert 'http_requests_total{handler="/health"' in response.text or (
        "http_requests_total" in response.text
    )


def test_scoring_request_increments_scoring_counter(client, real_ids):
    before = _get_counter_value(client.get("/metrics").text, "finguard_scoring_requests_total")

    client.post("/score-transaction", json={
        "transaction_id": "TX_METRICS_COUNTER_TEST",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": 42.0, "country": "US", "device_id": "DEV_METRICS_TEST",
    })

    after = _get_counter_value(client.get("/metrics").text, "finguard_scoring_requests_total")
    assert after == before + 1.0


def test_scoring_request_records_a_risk_level(client, real_ids):
    client.post("/score-transaction", json={
        "transaction_id": "TX_METRICS_RISK_LEVEL_TEST",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": 42.0, "country": "US", "device_id": "DEV_METRICS_TEST",
    })
    response = client.get("/metrics")
    assert "finguard_risk_level_total{risk_level=" in response.text


def test_unknown_customer_scoring_increments_error_counter(client):
    before_text = client.get("/metrics").text
    before = _get_counter_value(
        before_text, 'finguard_scoring_errors_total{error_type="unknown_customer"}'
    ) or 0.0

    client.post("/score-transaction", json={
        "transaction_id": "TX_METRICS_ERROR_TEST",
        "account_id": "ACCT_DOES_NOT_EXIST", "customer_id": "CUST_DOES_NOT_EXIST",
        "merchant_id": "MERCH_DOES_NOT_EXIST", "amount": 10.0,
        "country": "FR", "device_id": "D1",
    })

    after = _get_counter_value(
        client.get("/metrics").text, 'finguard_scoring_errors_total{error_type="unknown_customer"}'
    )
    assert after == before + 1.0


def test_customer_risk_lookup_increments_lookup_counter(client, real_ids):
    before = _get_counter_value(
        client.get("/metrics").text, 'finguard_customer_risk_lookups_total{found="True"}'
    ) or 0.0

    client.get(f"/customers/{real_ids['customer_id']}/risk")

    after = _get_counter_value(
        client.get("/metrics").text, 'finguard_customer_risk_lookups_total{found="True"}'
    )
    assert after == before + 1.0


def test_alerts_listed_counter_increases_by_result_count(client):
    before = _get_counter_value(client.get("/metrics").text, "finguard_alerts_listed_total") or 0.0

    response = client.get("/alerts?limit=3")
    n_returned = len(response.json())

    after = _get_counter_value(client.get("/metrics").text, "finguard_alerts_listed_total")
    assert after == before + n_returned


def test_scoring_latency_histogram_has_observations(client, real_ids):
    client.post("/score-transaction", json={
        "transaction_id": "TX_METRICS_LATENCY_TEST",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": 42.0, "country": "US", "device_id": "DEV_METRICS_TEST",
    })
    response = client.get("/metrics")
    count = _get_counter_value(response.text, "finguard_scoring_latency_seconds_count")
    assert count is not None and count >= 1.0
