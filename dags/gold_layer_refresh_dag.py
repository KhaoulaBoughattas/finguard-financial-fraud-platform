"""gold_layer_refresh DAG.

Kafka -> Spark -> Delta Bronze/Silver is the eventual source (Phase 3), but
since Delta isn't runnable in this sandbox (see docs/spark-streaming.md
§7), this DAG's "refresh" step generates fresh synthetic data straight
into Postgres (Phase 1's --persist-db path) as the Phase 4 dbt source --
consistent with the same documented substitution used throughout
docs/dbt.md. In a deployment where Delta is available, the first task
would instead be "wait for/read the latest Silver Delta partition," not
regenerate data -- the dbt run/test tasks are unchanged either way.

Idempotency: generation uses ON CONFLICT DO NOTHING (safe to rerun); dbt
run/test are naturally idempotent (dbt recomputes each model's full
result on every run -- none of these models are configured as
incremental, so there is no partial-mutation state to worry about).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dags.common import DEFAULT_ARGS, postgres_env

DBT_PROJECT_DIR = "/home/claude/finguard-fraud-platform/dbt"


def _generate_and_persist(**context) -> dict:
    from src.generator.generate import run as generate_run

    summary = generate_run(
        num_customers=2000, num_accounts_max_per_customer=2, num_merchants=300,
        num_transactions=5000, fraud_rate=0.015, seed=42,
        output_dir="/tmp/finguard_airflow_data", persist_db=True,
    )
    context["ti"].xcom_push(key="generation_summary", value={
        k: v for k, v in summary.items() if k != "fraud_pattern_counts"
    })
    return summary


with DAG(
    dag_id="gold_layer_refresh",
    description="Refresh reference data and rebuild the dbt Gold layer",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finguard", "dbt", "gold-layer"],
) as dag:
    generate_data = PythonOperator(
        task_id="generate_and_persist_reference_data",
        python_callable=_generate_and_persist,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
        env=postgres_env(),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
        env=postgres_env(),
    )

    generate_data >> dbt_run >> dbt_test
