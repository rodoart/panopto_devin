"""Tests for data-quality metrics."""

import pytest

from mecv.metrics.quality import (
    CardinalityRatioMetric,
    CategoryCompositionDriftMetric,
    DominantCategoryRateMetric,
    NullRateMetric,
    OutlierRateMetric,
)
from mecv.metrics.result import MetricResult


def _params(variable: str, var_type: str = "raw", data_type: str = "numeric"):
    return {
        "model_id": "M1",
        "information_date": "2025-01-01",
        "execution_id": "exec_001",
        "variable": variable,
        "var_type": var_type,
        "data_type": data_type,
    }


def test_null_rate(sample_data):
    """null_rate returns the fraction of null values and a RED status at the red threshold."""
    metric = NullRateMetric()
    result = metric.calculate(
        sample_data["raw"],
        sample_data["raw_baseline"],
        {"threshold_ambar": 0.05, "threshold_red": 0.10},
        **_params("age"),
    )
    assert isinstance(result, MetricResult)
    assert result.metric_name == "null_rate"
    assert result.metric_value == pytest.approx(0.10, abs=0.01)
    assert result.status == "RED"


def test_cardinality_ratio(sample_data):
    """cardinality_ratio returns distinct / total for the variable."""
    metric = CardinalityRatioMetric()
    result = metric.calculate(
        sample_data["raw"],
        None,
        {"threshold_red": 0.95},
        **_params("age"),
    )
    assert result.metric_name == "cardinality_ratio"
    # 9 distinct non-null ages / 10 rows
    assert result.metric_value == pytest.approx(0.90, abs=0.01)
    assert result.status == "GREEN"


def test_outlier_rate(sample_data):
    """outlier_rate flags values outside 1.5*IQR."""
    metric = OutlierRateMetric()
    result = metric.calculate(
        sample_data["raw"],
        None,
        {"threshold_ambar": 0.03, "threshold_red": 0.06},
        **_params("age"),
    )
    assert result.metric_name == "outlier_rate"
    # The 1000.0 row is the single outlier among 10 rows.
    assert result.metric_value == pytest.approx(0.10, abs=0.05)
    assert result.status == "RED"


def test_dominant_category_rate(sample_data):
    """dominant_category_rate returns the frequency of the most common category."""
    metric = DominantCategoryRateMetric()
    result = metric.calculate(
        sample_data["raw"],
        None,
        {"threshold_red": 0.90},
        **_params("category", var_type="raw", data_type="categorical"),
    )
    assert result.metric_name == "dominant_category_rate"
    # Category A has 4/10 rows.
    assert result.metric_value == pytest.approx(0.40, abs=0.01)
    assert result.status == "GREEN"


def test_category_composition_drift(sample_data):
    """category_composition_drift measures top-N Jaccard distance vs baseline."""
    metric = CategoryCompositionDriftMetric()
    result = metric.calculate(
        sample_data["raw"],
        sample_data["raw_baseline"],
        {"threshold_ambar": 0.10, "threshold_red": 0.30},
        **_params("category", var_type="raw", data_type="categorical"),
        top_n=2,
    )
    assert result.metric_name == "category_composition_drift"
    # Current top 2 = {A, B}; baseline top 2 = {C, B}; Jaccard = 1/3; drift = 2/3.
    assert result.metric_value == pytest.approx(0.67, abs=0.1)
    assert result.status == "RED"
