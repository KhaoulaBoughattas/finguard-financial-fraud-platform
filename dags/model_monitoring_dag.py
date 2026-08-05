"""model_monitoring DAG.

Lightweight, frequent-cadence monitoring: compares recent transaction
volume and fraud rate against the historical baseline, computed from real
queries against Postgres. This is a real, if intentionally simple, drift
signal -- not a claim that this replaces Phase 8's planned Prometheus/
Grafana stack (model-metric dashboards, latency, throughput). What this
DAG can do without that infrastructure: flag "the fraud rate this window
looks meaningfully different from history," which is exactly the kind of
check that should run often and cheaply, independent of the heavier
full-dataset model_validation DAG.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.common import DEFAULT_ARGS

FRAUD_RATE_DRIFT_RATIO_THRESHOLD = 3.0  # deliberately loose -- catch serious
# problems, not normal variance, same philosophy as model_validation's
# threshold, and equally worth tightening with real operational experience.
RECENT_WINDOW_TRANSACTION_COUNT = 500  # "recent" = the most recently
# inserted N transactions, not a wall-clock window -- this sandbox's
# synthetic data doesn't arrive on a real clock, so a row-count window is
# the meaningful equivalent here; a live deployment would use tx_timestamp.


def _check_for_drift(**context) -> dict:
    import psycopg

    from src.common.config import get_settings

    settings = get_settings().postgres
    conn = psycopg.connect(
        host=settings.host, port=settings.port, dbname=settings.db,
        user=settings.user, password=settings.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), avg(case when is_fraud then 1.0 else 0.0 end) FROM transactions;"
            )
            total_count, baseline_fraud_rate = cur.fetchone()

            cur.execute(
                "SELECT avg(case when is_fraud then 1.0 else 0.0 end) FROM ("
                "  SELECT is_fraud FROM transactions ORDER BY tx_timestamp DESC LIMIT %s"
                ") recent;",
                (RECENT_WINDOW_TRANSACTION_COUNT,),
            )
            recent_fraud_rate = cur.fetchone()[0]
    finally:
        conn.close()

    baseline_fraud_rate = float(baseline_fraud_rate or 0.0)
    recent_fraud_rate = float(recent_fraud_rate or 0.0)
    drift_ratio = (
        recent_fraud_rate / baseline_fraud_rate if baseline_fraud_rate > 0 else float("inf")
    )

    result = {
        "total_transactions": total_count,
        "baseline_fraud_rate": round(baseline_fraud_rate, 5),
        "recent_fraud_rate": round(recent_fraud_rate, 5),
        "drift_ratio": round(drift_ratio, 2) if drift_ratio != float("inf") else None,
        "recent_window_size": RECENT_WINDOW_TRANSACTION_COUNT,
    }
    context["ti"].xcom_push(key="monitoring_report", value=result)

    if drift_ratio >= FRAUD_RATE_DRIFT_RATIO_THRESHOLD or (
        baseline_fraud_rate == 0 and recent_fraud_rate > 0
    ):
        raise ValueError(
            f"Recent fraud rate ({recent_fraud_rate}) has drifted {drift_ratio}x from "
            f"the historical baseline ({baseline_fraud_rate}) -- exceeds the "
            f"{FRAUD_RATE_DRIFT_RATIO_THRESHOLD}x threshold: {result}"
        )
    return result


with DAG(
    dag_id="model_monitoring",
    description="Monitor recent fraud-rate drift against the historical baseline",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finguard", "ml", "monitoring"],
) as dag:
    check_drift = PythonOperator(
        task_id="check_for_fraud_rate_drift",
        python_callable=_check_for_drift,
    )
