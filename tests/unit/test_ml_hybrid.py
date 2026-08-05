import pandas as pd

from src.ml.hybrid import (
    apply_business_rules,
    assess_transaction,
    blend_scores,
    score_to_level,
)


def _row(**overrides) -> pd.Series:
    base = {
        "amount_to_avg_ratio": 1.0,
        "customer_txn_count_last_24h": 0,
        "is_cross_border": False,
        "is_unusual_hour": False,
        "merchant_risk_score_base": 0.05,
    }
    base.update(overrides)
    return pd.Series(base)


def test_large_amount_ratio_triggers_rule():
    row = _row(amount_to_avg_ratio=10.0)
    reasons = apply_business_rules(row)
    assert any("customer average" in r for r in reasons)


def test_normal_amount_ratio_does_not_trigger_rule():
    row = _row(amount_to_avg_ratio=1.2)
    reasons = apply_business_rules(row)
    assert not any("customer average" in r for r in reasons)


def test_high_velocity_triggers_rule():
    row = _row(customer_txn_count_last_24h=8)
    reasons = apply_business_rules(row)
    assert any("last 24h" in r for r in reasons)


def test_cross_border_and_unusual_hour_combination_triggers_rule():
    row = _row(is_cross_border=True, is_unusual_hour=True)
    reasons = apply_business_rules(row)
    assert any("cross-border" in r for r in reasons)


def test_cross_border_alone_does_not_trigger_combination_rule():
    row = _row(is_cross_border=True, is_unusual_hour=False)
    reasons = apply_business_rules(row)
    assert not any("cross-border" in r for r in reasons)


def test_high_risk_merchant_triggers_rule():
    row = _row(merchant_risk_score_base=0.4)
    reasons = apply_business_rules(row)
    assert any("high-risk category" in r for r in reasons)


def test_no_rules_triggered_for_normal_transaction():
    row = _row()
    reasons = apply_business_rules(row)
    assert reasons == []


def test_blend_scores_weights_xgboost_most_heavily():
    blended_high_xgb = blend_scores(rule_reasons=[], xgb_score=0.9, iforest_score=0.1)
    blended_high_iforest = blend_scores(rule_reasons=[], xgb_score=0.1, iforest_score=0.9)
    assert blended_high_xgb > blended_high_iforest


def test_score_to_level_thresholds():
    assert score_to_level(0.9) == "CRITICAL"
    assert score_to_level(0.6) == "HIGH"
    assert score_to_level(0.3) == "MEDIUM"
    assert score_to_level(0.1) == "LOW"


def test_assess_transaction_always_returns_at_least_one_reason():
    row = _row()
    assessment = assess_transaction(row, xgb_score=0.05, iforest_score=0.05)
    assert len(assessment.reasons) >= 1


def test_assess_transaction_reflects_high_model_scores_in_reasons():
    row = _row()
    assessment = assess_transaction(row, xgb_score=0.95, iforest_score=0.1)
    assert any("XGBoost" in r for r in assessment.reasons)
    assert assessment.risk_level in {"MEDIUM", "HIGH", "CRITICAL"}
