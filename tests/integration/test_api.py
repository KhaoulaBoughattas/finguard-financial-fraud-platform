"""API integration tests, using FastAPI's TestClient (in-process, no
running server needed -- a persistent background server process isn't
supported in this sandbox, and TestClient is the standard way to test a
FastAPI app regardless).

Skipped automatically if Postgres is unreachable or has no data, or if no
production model is registered in MLflow -- consistent with the
skip-if-unavailable pattern from every other infra-dependent test suite
in this project.
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
            cur.execute("SELECT transaction_id FROM transactions LIMIT 1")
            transaction_id = cur.fetchone()[0]
        return {
            "customer_id": customer_id, "account_id": account_id,
            "merchant_id": merchant_id, "transaction_id": transaction_id,
        }
    finally:
        conn.close()


def test_health_endpoint_reports_healthy(client):
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["database"] == "reachable"
    assert body["model"] == "loaded"
    assert body["status"] == "healthy"


def test_score_transaction_returns_full_assessment(client, real_ids):
    response = client.post("/score-transaction", json={
        "transaction_id": "TX_TEST_SCORE_001",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": 42.50,
        "country": "US",
        "device_id": "DEV_TEST",
    })
    assert response.status_code == 200
    body = response.json()
    assert body["transaction_id"] == "TX_TEST_SCORE_001"
    assert 0.0 <= body["risk_score"] <= 1.0
    assert body["risk_level"] in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
    assert body["decision"] in {"APPROVE", "REVIEW", "BLOCK"}
    assert len(body["reasons"]) >= 1


def test_score_transaction_flags_large_outlier_amount(client, real_ids):
    response = client.post("/score-transaction", json={
        "transaction_id": "TX_TEST_SCORE_002",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": 999999.0,
        "country": "US",
        "device_id": "DEV_TEST",
    })
    assert response.status_code == 200
    body = response.json()
    assert body["risk_level"] in {"HIGH", "CRITICAL"}
    assert any("average" in r for r in body["reasons"])


def test_score_transaction_unknown_customer_returns_404(client):
    response = client.post("/score-transaction", json={
        "transaction_id": "TX_TEST_UNKNOWN",
        "account_id": "ACCT_DOES_NOT_EXIST",
        "customer_id": "CUST_DOES_NOT_EXIST",
        "merchant_id": "MERCH_DOES_NOT_EXIST",
        "amount": 10.0,
        "country": "FR",
        "device_id": "D1",
    })
    assert response.status_code == 404


def test_score_transaction_rejects_negative_amount(client, real_ids):
    response = client.post("/score-transaction", json={
        "transaction_id": "TX_TEST_NEG",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": -5.0,
        "country": "FR",
        "device_id": "D1",
    })
    assert response.status_code == 422


def test_score_transaction_rejects_missing_required_field(client, real_ids):
    response = client.post("/score-transaction", json={
        "transaction_id": "TX_TEST_MISSING",
        "customer_id": real_ids["customer_id"],
        "amount": 10.0, "country": "FR", "device_id": "D1",
    })
    assert response.status_code == 422


def test_score_transaction_rejects_invalid_country_length(client, real_ids):
    response = client.post("/score-transaction", json={
        "transaction_id": "TX_TEST_BADCOUNTRY",
        "account_id": real_ids["account_id"],
        "customer_id": real_ids["customer_id"],
        "merchant_id": real_ids["merchant_id"],
        "amount": 10.0, "country": "FRANCE", "device_id": "D1",
    })
    assert response.status_code == 422


def test_get_customer_risk_returns_current_state(client, real_ids):
    response = client.get(f"/customers/{real_ids['customer_id']}/risk")
    assert response.status_code == 200
    body = response.json()
    assert body["customer_id"] == real_ids["customer_id"]
    assert body["is_current"] is True
    assert body["risk_level"] in {"LOW", "MEDIUM", "HIGH"}


def test_get_customer_risk_unknown_customer_returns_404(client):
    response = client.get("/customers/CUST_DOES_NOT_EXIST/risk")
    assert response.status_code == 404


def test_get_transaction_returns_full_record(client, real_ids):
    response = client.get(f"/transactions/{real_ids['transaction_id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["transaction_id"] == real_ids["transaction_id"]
    assert "amount" in body
    assert "is_fraud" in body


def test_get_transaction_unknown_id_returns_404(client):
    response = client.get("/transactions/TX_DOES_NOT_EXIST")
    assert response.status_code == 404


def test_list_alerts_returns_bounded_list(client):
    response = client.get("/alerts?limit=5")
    assert response.status_code == 200
    body = response.json()
    assert len(body) <= 5
    for alert in body:
        assert "alert_id" in alert
        assert "alert_source" in alert


def test_list_alerts_filters_by_customer_id(client, real_ids):
    response = client.get(f"/alerts?customer_id={real_ids['customer_id']}")
    assert response.status_code == 200
    body = response.json()
    for alert in body:
        assert alert["customer_id"] == real_ids["customer_id"]


def test_list_alerts_rejects_limit_over_max(client):
    response = client.get("/alerts?limit=9999")
    assert response.status_code == 422


def test_list_alerts_requires_api_key_when_one_is_configured(client, monkeypatch):
    from src.api import auth
    from src.common.config import get_settings

    monkeypatch.setenv("SECURITY_API_KEY", "test-integration-key")
    get_settings.cache_clear()
    auth._warned_no_key_configured = False
    try:
        response = client.get("/alerts?limit=1")  # no header
        assert response.status_code == 401

        response = client.get("/alerts?limit=1", headers={"X-API-Key": "wrong"})
        assert response.status_code == 401

        response = client.get("/alerts?limit=1", headers={"X-API-Key": "test-integration-key"})
        assert response.status_code == 200
    finally:
        monkeypatch.delenv("SECURITY_API_KEY", raising=False)
        get_settings.cache_clear()


def test_health_endpoint_never_requires_api_key(client, monkeypatch):
    """/health must stay reachable without auth -- monitoring systems
    (and Docker healthchecks) shouldn't need a secret to check liveness."""
    from src.common.config import get_settings

    monkeypatch.setenv("SECURITY_API_KEY", "some-key")
    get_settings.cache_clear()
    try:
        response = client.get("/health")  # no header sent
        assert response.status_code == 200
    finally:
        monkeypatch.delenv("SECURITY_API_KEY", raising=False)
        get_settings.cache_clear()
