"""Tests for stability metrics (PSI and KS)."""

import pytest

from mecv.metrics.result import MetricResult
from mecv.metrics.stability import KSMetric, PSICanonicalMetric, PSIDynamicMetric


def _params(variable: str, data_type: str = "numeric"):
    return {
        "model_id": "M1",
        "information_date": "2025-01-01",
        "execution_id": "exec_001",
        "variable": variable,
        "var_type": "raw",
        "data_type": data_type,
    }


def test_psi_canonical_with_baseline(sample_data):
    """psi_canonical computes a non-negative PSI against a baseline."""
    metric = PSICanonicalMetric()
    result = metric.calculate(
        sample_data["raw"],
        sample_data["raw_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params("age"),
    )
    assert isinstance(result, MetricResult)
    assert result.metric_name == "psi_canonical"
    assert result.metric_value >= 0.0
    assert result.baseline_value == 0.0


def test_psi_dynamic_requires_and_uses_baseline(sample_data):
    """psi_dynamic raises without a baseline and computes PSI when one is provided."""
    metric = PSIDynamicMetric()
    with pytest.raises(ValueError, match="baseline"):
        metric.calculate(
            sample_data["raw"],
            None,
            {"threshold_ambar": 0.10, "threshold_red": 0.20},
            **_params("age"),
        )

    result = metric.calculate(
        sample_data["raw"],
        sample_data["raw_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params("age"),
    )
    assert result.metric_name == "psi_dynamic"
    assert result.metric_value >= 0.0


def test_ks_vs_dev(sample_data):
    """ks_vs_dev returns the maximum CDF distance between current and baseline."""
    metric = KSMetric()
    result = metric.calculate(
        sample_data["raw"].select("age", "customer_id", "information_date"),
        sample_data["raw_baseline"].select("age", "customer_id", "information_date"),
        {"threshold_ambar": 0.10, "threshold_red": 0.20},
        **_params("age"),
    )
    assert result.metric_name == "ks_vs_dev"
    assert 0.0 <= result.metric_value <= 1.0
