"""model_training DAG.

Wraps the existing src.ml.train pipeline (Phase 5) -- no training logic is
duplicated here, this DAG's only job is scheduling/retries/logging around
an already-tested pipeline. Runs weekly by default (retraining a fraud
model daily on a slowly-changing synthetic dataset would just reproduce
the same model; a real deployment would trigger this on a genuine
schedule or data-volume trigger, not arbitrarily).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.common import DEFAULT_ARGS


def _train_model(**context) -> dict:
    from src.ml.train import run as train_run

    summary = train_run()
    summary.pop("sample_assessments", None)
    context["ti"].xcom_push(key="training_summary", value=summary)

    if summary["xgboost"]["pr_auc"] <= summary["baseline"]["pr_auc"]:
        raise ValueError(
            "Trained model did not beat the baseline on PR-AUC -- "
            f"xgboost={summary['xgboost']['pr_auc']} baseline={summary['baseline']['pr_auc']}"
        )
    return summary


with DAG(
    dag_id="model_training",
    description="Train and register the fraud detection models (Phase 5 pipeline)",
    default_args=DEFAULT_ARGS,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finguard", "ml", "training"],
) as dag:
    train_model = PythonOperator(
        task_id="train_and_register_model",
        python_callable=_train_model,
    )
