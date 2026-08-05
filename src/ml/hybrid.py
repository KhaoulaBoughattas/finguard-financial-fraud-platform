"""Hybrid fraud risk engine: business rules + XGBoost + Isolation Forest.

Per Phase 0's explicit requirement: "Do NOT only return a probability. Make
the system explainable." Every scored transaction gets a risk_level AND a
list of human-readable reasons, not just a number.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

RULE_AMOUNT_RATIO_THRESHOLD = 8.0  # matches the generator's own
# LARGE_AMOUNT_OUTLIER pattern (8-15x customer average, see
# src/generator/fraud_patterns.py) -- the rule is deliberately calibrated
# against the same threshold the synthetic data's ground truth uses.
RULE_VELOCITY_THRESHOLD = 5  # transactions in the last 24h


@dataclass
class RiskAssessment:
    risk_score: float  # blended, in [0, 1]
    risk_level: str  # LOW / MEDIUM / HIGH / CRITICAL
    reasons: list[str]


def apply_business_rules(row: pd.Series) -> list[str]:
    reasons = []
    if row["amount_to_avg_ratio"] >= RULE_AMOUNT_RATIO_THRESHOLD:
        reasons.append(
            f"transaction amount {row['amount_to_avg_ratio']:.1f}x customer average"
        )
    if row["customer_txn_count_last_24h"] >= RULE_VELOCITY_THRESHOLD:
        reasons.append(
            f"{int(row['customer_txn_count_last_24h'])} transactions by this customer in the last 24h"
        )
    if row["is_cross_border"] and row["is_unusual_hour"]:
        reasons.append("cross-border transaction at an unusual hour")
    if row["merchant_risk_score_base"] >= 0.25:
        reasons.append("merchant in a high-risk category")
    return reasons


def blend_scores(
    rule_reasons: list[str], xgb_score: float, iforest_score: float,
    rule_weight: float = 0.2, xgb_weight: float = 0.6, iforest_weight: float = 0.2,
) -> float:
    """Blends the three signals into one risk score.

    Weights favor XGBoost (the supervised model, trained directly on
    labeled fraud) but keep meaningful weight on the rules (catch known
    patterns immediately, no model latency) and Isolation Forest (catch
    behavior the supervised model has never seen, per Phase 0's stated
    purpose for including an unsupervised model at all). These weights are
    a starting point for tuning against business cost of false
    positives/negatives, not a claim of optimality.
    """
    rule_score = min(1.0, len(rule_reasons) / 3)  # 3+ triggered rules -> saturate at 1.0
    return (
        rule_weight * rule_score + xgb_weight * xgb_score + iforest_weight * iforest_score
    )


def score_to_level(score: float) -> str:
    if score >= 0.75:
        return "CRITICAL"
    if score >= 0.5:
        return "HIGH"
    if score >= 0.25:
        return "MEDIUM"
    return "LOW"


def assess_transaction(row: pd.Series, xgb_score: float, iforest_score: float) -> RiskAssessment:
    reasons = apply_business_rules(row)
    blended = blend_scores(reasons, xgb_score, iforest_score)
    level = score_to_level(blended)

    if xgb_score >= 0.7:
        reasons.append(f"XGBoost model score {xgb_score:.2f}")
    if iforest_score >= 0.7:
        reasons.append(f"anomaly score {iforest_score:.2f} (unusual feature combination)")
    if not reasons:
        reasons.append("no individual risk factors triggered")

    return RiskAssessment(risk_score=blended, risk_level=level, reasons=reasons)


def assess_batch(df: pd.DataFrame, xgb_scores: np.ndarray, iforest_scores: np.ndarray) -> pd.DataFrame:
    results = []
    for i, (_, row) in enumerate(df.iterrows()):
        assessment = assess_transaction(row, float(xgb_scores[i]), float(iforest_scores[i]))
        results.append({
            "transaction_id": row["transaction_id"],
            "risk_score": assessment.risk_score,
            "risk_level": assessment.risk_level,
            "reasons": assessment.reasons,
        })
    return pd.DataFrame(results)
