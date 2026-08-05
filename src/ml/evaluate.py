"""Evaluation metrics for the fraud classifier.

Fraud is highly imbalanced (~1.5% positive rate in this dataset) -- Phase 0
explicitly warns against relying on accuracy alone (a model that always
predicts "not fraud" would score ~98.5% accuracy while catching zero
fraud). Every function here returns the full picture: precision, recall,
F1, ROC-AUC, PR-AUC, confusion matrix, and false positive rate.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


@dataclass
class EvaluationResult:
    precision: float
    recall: float
    f1: float
    roc_auc: float
    pr_auc: float  # a.k.a. average precision -- the primary metric for
    # imbalanced classification, since ROC-AUC can look deceptively good
    # even for a weak model when negatives vastly outnumber positives.
    false_positive_rate: float
    confusion_matrix: np.ndarray = field(repr=False)
    threshold: float

    def as_dict(self) -> dict:
        tn, fp, fn, tp = self.confusion_matrix.ravel()
        return {
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "roc_auc": round(self.roc_auc, 4),
            "pr_auc": round(self.pr_auc, 4),
            "false_positive_rate": round(self.false_positive_rate, 4),
            "threshold": self.threshold,
            "true_negatives": int(tn),
            "false_positives": int(fp),
            "false_negatives": int(fn),
            "true_positives": int(tp),
        }


def evaluate_predictions(
    y_true: np.ndarray, y_score: np.ndarray, threshold: float = 0.5
) -> EvaluationResult:
    """y_score: predicted probability/anomaly score in [0,1] (higher =
    more likely fraud). threshold: cutoff for converting scores to a
    binary fraud/not-fraud decision -- exposed as a parameter, not
    hardcoded, since the right threshold is a business trade-off
    (precision vs. recall), not a fixed constant."""
    y_pred = (y_score >= threshold).astype(int)
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, _fn, _tp = cm.ravel()
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

    return EvaluationResult(
        precision=precision_score(y_true, y_pred, zero_division=0),
        recall=recall_score(y_true, y_pred, zero_division=0),
        f1=f1_score(y_true, y_pred, zero_division=0),
        roc_auc=roc_auc_score(y_true, y_score) if len(set(y_true)) > 1 else float("nan"),
        pr_auc=average_precision_score(y_true, y_score) if len(set(y_true)) > 1 else float("nan"),
        false_positive_rate=fpr,
        confusion_matrix=cm,
        threshold=threshold,
    )


def baseline_majority_class(y_true: np.ndarray) -> EvaluationResult:
    """The baseline every real model must beat: predict "not fraud" for
    every transaction. Included so model metrics are never reported in
    isolation -- Phase 0 requires comparing against a baseline, not just
    reporting a number that sounds good on its own."""
    y_score = np.zeros_like(y_true, dtype=float)
    return evaluate_predictions(y_true, y_score, threshold=0.5)
