"""MLflow experiment tracking and model registry.

Implements the Candidate -> Staging -> Production promotion workflow from
Phase 0. Uses a local SQLite-backed tracking store (see MLSettings in
src/common/config.py) -- fully local, no server process or network
required, which is what makes this phase runnable end-to-end in this
sandbox unlike Kafka/Delta in Phases 2-3.
"""
from __future__ import annotations

import mlflow
from mlflow.tracking import MlflowClient

from src.common.config import MLSettings
from src.common.logging import get_logger

logger = get_logger(__name__)


def configure_mlflow(settings: MLSettings) -> None:
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.experiment_name)


def register_candidate(
    model_name: str, model_uri: str, run_id: str
) -> str:
    """Registers a trained model as a new version in the MLflow Model
    Registry, in the 'Candidate' stage (via an alias, since MLflow
    deprecated the old numeric stage transitions in favor of aliases in
    recent versions -- see docs/ml.md).

    `model_uri` must be the URI returned by the logging call itself
    (e.g. `mlflow.xgboost.log_model(...).model_uri`) -- NOT reconstructed
    as `runs:/{run_id}/model`. That legacy pattern assumes artifacts are
    stored directly under the run's own artifact directory; MLflow 3.x's
    `log_model(name=...)` instead creates a separate "Logged Model" entity
    with its own `models:/m-<id>` URI (a real compatibility bug found and
    fixed during Phase 6 -- see docs/adr/008-airflow-orchestration.md).

    Returns the registered model version string.
    """
    client = MlflowClient()

    try:
        client.create_registered_model(model_name)
    except Exception as exc:  # noqa: BLE001 -- "already exists" is the expected/common
        # case after the first registration; any other failure will surface
        # again (loudly) on the next call that actually needs the model to exist.
        logger.debug("create_registered_model skipped", extra={"context": {"error": str(exc)}})

    model_version = client.create_model_version(
        name=model_name, source=model_uri, run_id=run_id,
    )
    client.set_registered_model_alias(model_name, "candidate", model_version.version)
    logger.info(
        "Model registered as candidate",
        extra={"context": {"model_name": model_name, "version": model_version.version}},
    )
    return model_version.version


def promote_to_staging(model_name: str, version: str) -> None:
    client = MlflowClient()
    client.set_registered_model_alias(model_name, "staging", version)
    logger.info("Model promoted to staging", extra={"context": {"model_name": model_name, "version": version}})


def promote_to_production(model_name: str, version: str) -> None:
    client = MlflowClient()
    client.set_registered_model_alias(model_name, "production", version)
    logger.info("Model promoted to production", extra={"context": {"model_name": model_name, "version": version}})


def get_production_model_uri(model_name: str) -> str:
    return f"models:/{model_name}@production"
