import numpy as np

from src.ml.evaluate import baseline_majority_class, evaluate_predictions


def test_perfect_predictions_score_maximally():
    y_true = np.array([0, 0, 0, 1, 1])
    y_score = np.array([0.0, 0.0, 0.0, 1.0, 1.0])
    result = evaluate_predictions(y_true, y_score)
    assert result.precision == 1.0
    assert result.recall == 1.0
    assert result.f1 == 1.0
    assert result.false_positive_rate == 0.0


def test_confusion_matrix_values_are_correct():
    # 2 true negatives, 1 false positive, 1 false negative, 1 true positive
    y_true = np.array([0, 0, 0, 1, 1])
    y_score = np.array([0.0, 0.0, 0.9, 0.0, 0.9])
    result = evaluate_predictions(y_true, y_score)
    d = result.as_dict()
    assert d["true_negatives"] == 2
    assert d["false_positives"] == 1
    assert d["false_negatives"] == 1
    assert d["true_positives"] == 1


def test_false_positive_rate_computed_correctly():
    # 3 negatives, 1 of which is misclassified as positive -> FPR = 1/3
    y_true = np.array([0, 0, 0, 1])
    y_score = np.array([0.0, 0.9, 0.0, 0.9])
    result = evaluate_predictions(y_true, y_score)
    assert abs(result.false_positive_rate - (1 / 3)) < 1e-9


def test_threshold_changes_classification():
    y_true = np.array([0, 1])
    y_score = np.array([0.4, 0.6])
    low_threshold = evaluate_predictions(y_true, y_score, threshold=0.3)
    high_threshold = evaluate_predictions(y_true, y_score, threshold=0.9)
    # at threshold 0.3, both predicted positive -> the negative becomes a FP
    assert low_threshold.as_dict()["false_positives"] == 1
    # at threshold 0.9, neither predicted positive -> the positive becomes a FN
    assert high_threshold.as_dict()["false_negatives"] == 1


def test_baseline_majority_class_predicts_all_negative():
    y_true = np.array([0, 0, 0, 1, 1])
    result = baseline_majority_class(y_true)
    d = result.as_dict()
    assert d["true_positives"] == 0
    assert d["false_positives"] == 0
    assert result.recall == 0.0
    assert result.precision == 0.0


def test_pr_auc_reflects_ranking_quality_not_just_threshold():
    """A model that ranks fraud higher (even if the absolute threshold at
    0.5 misses some) should still show a meaningfully higher PR-AUC than
    one with no discriminative power -- this is the whole reason PR-AUC
    (not just precision/recall at one threshold) matters for imbalanced
    classification."""
    y_true = np.array([0, 0, 0, 0, 1, 1])
    good_scores = np.array([0.1, 0.15, 0.2, 0.25, 0.8, 0.9])  # fraud ranks highest
    random_scores = np.array([0.5, 0.5, 0.5, 0.5, 0.5, 0.5])  # no discrimination
    good_result = evaluate_predictions(y_true, good_scores)
    random_result = evaluate_predictions(y_true, random_scores)
    assert good_result.pr_auc > random_result.pr_auc
