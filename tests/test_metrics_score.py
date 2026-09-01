"""Tests for score distribution metrics."""

import pytest

from mecv.binning import numeric_bins
from mecv.metrics.result import MetricResult
from mecv.metrics.score import (
    ApprovalRateMetric,
    ConcentrationGiniMetric,
    EntropyMetric,
    PSIApprovedMetric,
    PSIRejectedMetric,
    ScoreRangeMetric,
    ScoreTailShiftMetric,
)


def _params():
    return {
        "model_id": "M1",
        "information_date": "2025-01-01",
        "execution_id": "exec_001",
        "variable": "score",
        "var_type": "score",
        "data_type": "numeric",
        "score_col": "score",
        "target_col": "target",
    }


def test_range_violation(sample_data):
    """range_violation counts scores outside [0, 1]."""
    metric = ScoreRangeMetric()
    result = metric.calculate(
        sample_data["score"],
        sample_data["score_baseline"],
        {"threshold_red": 1e-9},
        **_params(),
    )
    assert isinstance(result, MetricResult)
    assert result.metric_name == "range_violation"
    # Two rows violate the [0, 1] range out of 10.
    assert result.metric_value == pytest.approx(0.20, abs=0.01)
    assert result.status == "RED"


def test_entropy(sample_data):
    """entropy returns the score distribution entropy, optionally relative to baseline."""
    metric = EntropyMetric()
    result = metric.calculate(
        sample_data["score"],
        sample_data["score_baseline"],
        {"threshold_ambar": 0.15, "threshold_red": 0.30},
        **_params(),
    )
    assert result.metric_name == "entropy"
    assert result.metric_value >= 0.0


def test_approval_rate(sample_data):
    """approval_rate measures the share of scores above the cutoff."""
    metric = ApprovalRateMetric()
    result = metric.calculate(
        sample_data["score"],
        sample_data["score_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
        cut_off_probability=0.5,
    )
    assert result.metric_name == "approval_rate"
    assert result.metric_value >= 0.0


def test_tail_shift(sample_data):
    """tail_shift requires a baseline and computes p10/p90 movement."""
    metric = ScoreTailShiftMetric()
    with pytest.raises(ValueError, match="baseline"):
        metric.calculate(sample_data["score"], None, {}, **_params())

    result = metric.calculate(
        sample_data["score"],
        sample_data["score_baseline"],
        {"threshold_ambar": 0.05, "threshold_red": 0.10},
        **_params(),
    )
    assert result.metric_name == "tail_shift"
    assert result.metric_value >= 0.0


def test_concentration_gini(sample_data):
    """concentration_gini computes the Gini coefficient of the score distribution."""
    metric = ConcentrationGiniMetric()
    result = metric.calculate(
        sample_data["score"],
        sample_data["score_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
    )
    assert result.metric_name == "concentration_gini"
    assert result.metric_value >= 0.0


def test_psi_approved_and_rejected(sample_data):
    """psi_approved/rejected split scores at the cutoff and compute PSI against baseline."""
    baseline = sample_data["score_baseline"]
    bins = numeric_bins(baseline, "score", n_bins=10)

    approved_metric = PSIApprovedMetric()
    rejected_metric = PSIRejectedMetric()

    approved_result = approved_metric.calculate(
        sample_data["score"],
        baseline,
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
        cut_off_probability=0.5,
        bins=bins,
    )
    rejected_result = rejected_metric.calculate(
        sample_data["score"],
        baseline,
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params(),
        cut_off_probability=0.5,
        bins=bins,
    )
    assert approved_result.metric_name == "psi_approved"
    assert rejected_result.metric_name == "psi_rejected"
    assert approved_result.metric_value >= 0.0
    assert rejected_result.metric_value >= 0.0

    with pytest.raises(ValueError, match="bins"):
        approved_metric.calculate(
            sample_data["score"],
            baseline,
            {},
            **_params(),
            cut_off_probability=0.5,
            bins=[],
        )
