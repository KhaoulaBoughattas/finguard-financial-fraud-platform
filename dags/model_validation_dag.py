"""model_validation DAG.

Distinct from model_training: this DAG does NOT train anything. It loads
whatever model currently holds the 'production' alias in the MLflow
registry and re-evaluates it against the LATEST data, to catch model
degradation between training runs. A real deployment would run this far
more often than model_training itself, for exactly that reason.

HONEST CAVEAT: "latest data" here means the full current contents of the
Postgres transactions table -- which includes rows the production model
was originally trained on, since this sandbox has no live stream of
genuinely new, post-training transactions to evaluate against. This DAG
is therefore a degradation/sanity SMOKE CHECK (would this model still
clear a basic bar against roughly the data it knows), not true
out-of-sample validation against unseen future data. In a real deployment
with an ongoing transaction stream, this task would instead query only
transactions with tx_timestamp after the model's training cutoff.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.common import DEFAULT_ARGS

MODEL_NAME = "finguard-fraud-xgboost"
MIN_ACCEPTABLE_PR_AUC = 0.5  # far below the ~0.93 this model actually
# achieves in training (see docs/ml.md) -- deliberately loose, since this
# check exists to catch serious degradation (a broken pipeline, a data
# corruption issue), not to re-litigate normal run-to-run variance. A
# tighter, business-calibrated threshold is future work, same caveat as
# the promotion-gate multiplier in src/ml/train.py.


def _validate_production_model(**context) -> dict:
    import mlflow

    from src.common.config import get_settings
    from src.ml.data import load_training_data
    from src.ml.evaluate import evaluate_predictions
    from src.ml.features import FEATURE_COLUMNS, LABEL_COLUMN, engineer_features
    from src.ml.mlflow_tracking import configure_mlflow, get_production_model_uri

    settings = get_settings()
    configure_mlflow(settings.ml)

    model_uri = get_production_model_uri(MODEL_NAME)
    model = mlflow.pyfunc.load_model(model_uri)

    raw_df = load_training_data(settings.postgres)
    featured_df = engineer_features(raw_df)

    X = featured_df[FEATURE_COLUMNS]
    y = featured_df[LABEL_COLUMN].astype(int).to_numpy()
    scores = model.predict(X)

    result = evaluate_predictions(y, scores).as_dict()
    context["ti"].xcom_push(key="validation_report", value=result)

    if result["pr_auc"] < MIN_ACCEPTABLE_PR_AUC:
        raise ValueError(
            f"Production model PR-AUC ({result['pr_auc']}) fell below the "
            f"minimum acceptable threshold ({MIN_ACCEPTABLE_PR_AUC}) -- possible model "
            "degradation or data drift. See docs/ml.md for the monitoring caveats."
        )
    return result


with DAG(
    dag_id="model_validation",
    description="Re-validate the current production model against the latest data",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finguard", "ml", "validation"],
) as dag:
    validate_model = PythonOperator(
        task_id="validate_production_model",
        python_callable=_validate_production_model,
    )
