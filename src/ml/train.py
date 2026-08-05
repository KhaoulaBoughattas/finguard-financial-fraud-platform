"""Phase 5 training pipeline entrypoint.

Usage:
    python -m src.ml.train

Runs the full pipeline: load -> engineer features -> temporal split ->
train baseline/XGBoost/IsolationForest/hybrid -> evaluate all four on the
held-out TEST set -> log everything to MLflow -> register the XGBoost
model and (if it clears a minimum bar vs. baseline) promote it through
Candidate -> Staging -> Production.

Every metric reported is computed here and printed at the end -- nothing
in this module or docs/ml.md should be read as a metric unless it appears
in that final printed summary, which is generated from an actual run, not
hand-written.
"""
from __future__ import annotations

import mlflow
import mlflow.sklearn
import mlflow.xgboost
import numpy as np

from src.common.config import get_settings
from src.common.logging import configure_logging, get_logger
from src.ml.data import load_training_data
from src.ml.evaluate import baseline_majority_class, evaluate_predictions
from src.ml.features import FEATURE_COLUMNS, LABEL_COLUMN, engineer_features
from src.ml.hybrid import assess_batch, blend_scores
from src.ml.mlflow_tracking import (
    configure_mlflow,
    promote_to_production,
    promote_to_staging,
    register_candidate,
)
from src.ml.split import temporal_split
from src.ml.train_supervised import (
    get_feature_importance,
    predict_scores,
    train_xgboost,
)
from src.ml.train_unsupervised import predict_anomaly_scores, train_isolation_forest

logger = get_logger(__name__)

MODEL_NAME = "finguard-fraud-xgboost"

MIN_PR_AUC_IMPROVEMENT_MULTIPLIER = 5.0


def run() -> dict:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_mlflow(settings.ml)

    logger.info("Loading training data from Postgres")
    raw_df = load_training_data(settings.postgres)
    logger.info("Engineering features", extra={"context": {"rows": len(raw_df)}})
    featured_df = engineer_features(raw_df)

    train_df, val_df, test_df = temporal_split(featured_df)
    logger.info(
        "Temporal split complete",
        extra={"context": {"train": len(train_df), "val": len(val_df), "test": len(test_df)}},
    )

    y_test = test_df[LABEL_COLUMN].astype(int).to_numpy()

    with mlflow.start_run(run_name="fraud-detection-training") as run_ctx:
        run_id = run_ctx.info.run_id

        mlflow.log_params({
            "train_rows": len(train_df), "val_rows": len(val_df), "test_rows": len(test_df),
            "feature_columns": ",".join(FEATURE_COLUMNS),
            "random_seed": settings.ml.random_seed,
        })

        baseline_result = baseline_majority_class(y_test)
        mlflow.log_metrics({f"baseline_{k}": v for k, v in baseline_result.as_dict().items()
                             if isinstance(v, (int, float))})

        xgb_model = train_xgboost(train_df, val_df, random_seed=settings.ml.random_seed)
        xgb_test_scores = predict_scores(xgb_model, test_df)
        xgb_result = evaluate_predictions(y_test, xgb_test_scores)
        mlflow.log_metrics({f"xgboost_{k}": v for k, v in xgb_result.as_dict().items()
                             if isinstance(v, (int, float))})
        model_info = mlflow.xgboost.log_model(xgb_model, name="model")
        feature_importance = get_feature_importance(xgb_model)
        for feat, importance in feature_importance.items():
            mlflow.log_metric(f"importance_{feat}", importance)

        iforest_model = train_isolation_forest(train_df, random_seed=settings.ml.random_seed)
        iforest_test_scores = predict_anomaly_scores(iforest_model, test_df)
        iforest_result = evaluate_predictions(y_test, iforest_test_scores)
        mlflow.log_metrics({f"isolation_forest_{k}": v for k, v in iforest_result.as_dict().items()
                             if isinstance(v, (int, float))})
        mlflow.sklearn.log_model(iforest_model, name="isolation_forest_model")

        hybrid_scores = np.array([
            blend_scores(
                rule_reasons=[],
                xgb_score=xgb_test_scores[i], iforest_score=iforest_test_scores[i],
            )
            for i in range(len(test_df))
        ])
        hybrid_result = evaluate_predictions(y_test, hybrid_scores)
        mlflow.log_metrics({f"hybrid_{k}": v for k, v in hybrid_result.as_dict().items()
                             if isinstance(v, (int, float))})

        summary = {
            "baseline": baseline_result.as_dict(),
            "xgboost": xgb_result.as_dict(),
            "isolation_forest": iforest_result.as_dict(),
            "hybrid": hybrid_result.as_dict(),
            "feature_importance": feature_importance,
            "run_id": run_id,
        }

        version = register_candidate(
            MODEL_NAME, model_info.model_uri, run_id,
        )
        summary["registered_version"] = version

        improvement = (
            xgb_result.pr_auc / baseline_result.pr_auc
            if baseline_result.pr_auc and baseline_result.pr_auc > 0
            else float("inf")
        )
        summary["pr_auc_improvement_multiplier"] = round(improvement, 2)

        if improvement >= MIN_PR_AUC_IMPROVEMENT_MULTIPLIER:
            promote_to_staging(MODEL_NAME, version)
            promote_to_production(MODEL_NAME, version)
            summary["promotion"] = "candidate -> staging -> production"
        else:
            summary["promotion"] = (
                f"NOT promoted -- PR-AUC improvement ({improvement:.2f}x) below the "
                f"{MIN_PR_AUC_IMPROVEMENT_MULTIPLIER}x minimum bar"
            )

        sample_assessments = assess_batch(
            test_df.head(20).reset_index(drop=True),
            xgb_test_scores[:20], iforest_test_scores[:20],
        )
        summary["sample_assessments"] = sample_assessments.to_dict(orient="records")

    logger.info("Training run complete", extra={"context": {"run_id": run_id}})
    return summary


def main() -> None:
    import json
    summary = run()
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
