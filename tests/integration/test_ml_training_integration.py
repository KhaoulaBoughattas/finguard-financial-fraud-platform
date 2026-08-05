"""Integration test for the full Phase 5 training pipeline.

Skipped automatically if Postgres is unreachable -- same pattern as the
Phase 1/4 Postgres integration tests. Unlike Phase 2/3's Kafka/Delta
tests, this one is expected to actually RUN (not skip) in this sandbox,
since Postgres + MLflow (SQLite-backed) have no external network
dependency.

IMPORTANT test-isolation note: this module generates its OWN transaction
data (via the fixture below) immediately before running, rather than
depending on whatever happens to already be in the shared `transactions`
table. A real bug was found during Phase 5: test_db_integration.py's
fixture TRUNCATEs the shared Postgres tables as part of ITS setup: when
both test files ran in the same pytest session, this file's tests would
sometimes fail (if db_integration's truncation happened to run between
collection and execution) or misleadingly skip (if it ran before
collection) depending on file/test execution order -- a classic
shared-mutable-fixture hazard. Fixed by making this module self-contained:
it no longer trusts leftover data from manual `generate` runs or other
tests, and instead ensures its own sufficient dataset exists right before
each test needs it.
"""
import pytest

from src.common.config import get_settings


def _postgres_reachable() -> bool:
    try:
        import psycopg
        settings = get_settings()
        conn = psycopg.connect(
            host=settings.postgres.host, port=settings.postgres.port,
            dbname=settings.postgres.db, user=settings.postgres.user,
            password=settings.postgres.password,
        )
        conn.close()
        return True
    except Exception:  # noqa: BLE001 -- availability probe, same rationale as
        # the other Postgres/Kafka/Delta ones in this test suite.
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_reachable(), reason="PostgreSQL not reachable",
)


@pytest.fixture
def isolated_mlflow_tracking(tmp_path, monkeypatch):
    """Points MLflow at a throwaway SQLite DB for this test run, so it
    doesn't pollute the developer's real data/mlflow/mlflow.db."""
    db_path = tmp_path / "test_mlflow.db"
    monkeypatch.setenv("ML_MLFLOW_TRACKING_URI", f"sqlite:///{db_path}")
    monkeypatch.setenv("ML_EXPERIMENT_NAME", "test-experiment")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def sufficient_transaction_data():
    """Ensures enough transaction data exists for a meaningful train/val/
    test split, generated fresh right here rather than trusted from
    external state -- see the module docstring for why."""
    from src.generator.generate import run as generate_run

    generate_run(
        num_customers=500, num_accounts_max_per_customer=1, num_merchants=50,
        num_transactions=5000, fraud_rate=0.02, seed=123,
        output_dir="/tmp/finguard_ml_test_data", persist_db=True,
    )


def test_full_training_pipeline_runs_and_beats_baseline(
    isolated_mlflow_tracking, sufficient_transaction_data
):
    from src.ml.train import run

    summary = run()

    assert summary["xgboost"]["pr_auc"] > summary["baseline"]["pr_auc"]
    assert summary["xgboost"]["f1"] > 0
    assert summary["registered_version"] is not None
    assert "run_id" in summary


def test_hybrid_engine_produces_explainable_reasons_for_sample_transactions(
    isolated_mlflow_tracking, sufficient_transaction_data
):
    from src.ml.train import run

    summary = run()
    assert len(summary["sample_assessments"]) > 0
    for assessment in summary["sample_assessments"]:
        assert assessment["risk_level"] in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        assert len(assessment["reasons"]) >= 1
