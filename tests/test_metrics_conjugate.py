"""Tests for conjugate (score + target) performance metrics."""

import pytest

from mecv.metrics.conjugate import (
    AUCMetric,
    BrierScoreMetric,
    CalibrationSlopeMetric,
    GiniMetric,
    KSScoreTargetMetric,
    LiftTopDecileMetric,
)
from mecv.metrics.result import MetricResult


def _params():
    return {
        "model_id": "M1",
        "information_date": "2025-01-01",
        "execution_id": "exec_001",
        "variable": "__SCORE__",
        "var_type": "conjugate",
        "data_type": "numeric",
        "score_col": "score",
        "target_col": "target",
    }


def test_auc(sample_data):
    """auc returns a value between 0 and 1 for a score/target DataFrame."""
    metric = AUCMetric()
    result = metric.calculate(
        sample_data["joined"],
        sample_data["joined_baseline"],
        {},
        **_params(),
    )
    assert isinstance(result, MetricResult)
    assert result.metric_name == "auc"
    assert 0.0 <= result.metric_value <= 1.0
    assert result.baseline_value is not None


def test_gini(sample_data):
    """gini returns a performance metric derived from AUC."""
    metric = GiniMetric()
    result = metric.calculate(
        sample_data["joined"],
        sample_data["joined_baseline"],
        {"threshold_ambar": 0.05, "threshold_red": 0.10},
        **_params(),
    )
    assert result.metric_name == "gini"
    assert result.metric_value >= 0.0


def test_brier_score(sample_data):
    """brier_score computes mean((score - target)^2)."""
    metric = BrierScoreMetric()
    result = metric.calculate(
        sample_data["joined"],
        sample_data["joined_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
    )
    assert result.metric_name == "brier_score"
    assert result.metric_value >= 0.0


def test_lift_top_decile(sample_data):
    """lift_top_decile compares event rate in the top decile vs overall."""
    metric = LiftTopDecileMetric()
    result = metric.calculate(
        sample_data["joined"],
        sample_data["joined_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
    )
    assert result.metric_name == "lift_top_decile"
    assert result.metric_value >= 0.0


def test_calibration_slope(sample_data):
    """calibration_slope fits a linear regression of target on score."""
    metric = CalibrationSlopeMetric()
    result = metric.calculate(
        sample_data["joined"],
        sample_data["joined_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
    )
    assert result.metric_name == "calibration_slope"
    assert result.metric_value >= 0.0


def test_ks_score_target(sample_data):
    """ks_score_target computes the Kolmogorov-Smirnov statistic by target."""
    metric = KSScoreTargetMetric()
    result = metric.calculate(
        sample_data["joined"],
        sample_data["joined_baseline"],
        {"threshold_ambar": 0.05, "threshold_red": 0.10},
        **_params(),
    )
    assert result.metric_name == "ks_score_target"
    assert 0.0 <= result.metric_value <= 1.0
