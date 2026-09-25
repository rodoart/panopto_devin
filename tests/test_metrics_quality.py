"""Tests for data-quality metrics."""

import pytest

from panopto.metrics.quality import (
    CardinalityRatioMetric,
    CategoryCompositionDriftMetric,
    CompletenessMetric,
    DominantCategoryRateMetric,
    MedianShiftMetric,
    NullRateMetric,
    OutlierRateMetric,
    PopulationVariationMetric,
)
from panopto.metrics.result import MetricResult


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


def test_median_shift_requires_baseline(sample_data):
    """median_shift raises without a baseline dataframe."""
    metric = MedianShiftMetric()
    with pytest.raises(ValueError):
        metric.calculate(sample_data["raw"], None, {"threshold_ambar": 0.10, "threshold_red": 0.20}, **_params("age"))


def test_median_shift(sample_data):
    """median_shift returns the relative change of the median vs. baseline and flags it."""
    metric = MedianShiftMetric()
    result = metric.calculate(
        sample_data["raw"],
        sample_data["raw_baseline"],
        {"threshold_ambar": 0.05, "threshold_red": 0.10},
        **_params("age"),
    )
    assert result.metric_name == "median_shift"
    assert result.metric_value >= 0.0
    assert result.baseline_value is not None
    assert result.status in ("RED", "AMBAR", "GREEN")


def test_population_variation_requires_baseline(sample_data):
    """population_variation raises without a baseline dataframe."""
    metric = PopulationVariationMetric()
    with pytest.raises(ValueError):
        metric.calculate(
            sample_data["raw"], None, {"threshold_ambar": 0.15, "threshold_red": 0.30}, **_params("age")
        )


def test_population_variation_same_size_is_green(sample_data):
    """population_variation is 0 (GREEN) when the population size is unchanged."""
    metric = PopulationVariationMetric()
    result = metric.calculate(
        sample_data["raw"],
        sample_data["raw"],
        {"threshold_ambar": 0.15, "threshold_red": 0.30},
        **_params("age"),
    )
    assert result.metric_name == "population_variation"
    assert result.metric_value == pytest.approx(0.0, abs=1e-9)
    assert result.status == "GREEN"
    assert result.baseline_value == pytest.approx(10.0)


def test_population_variation_flags_large_drop(sample_data):
    """population_variation flags RED when the current population shrinks drastically."""
    metric = PopulationVariationMetric()
    shrunk = sample_data["raw"].limit(2)
    result = metric.calculate(
        shrunk,
        sample_data["raw"],
        {"threshold_ambar": 0.15, "threshold_red": 0.30},
        **_params("age"),
    )
    # |10/2 - 1| = 4.0
    assert result.metric_value == pytest.approx(4.0, abs=0.01)
    assert result.status == "RED"


def test_completeness_full_window(sample_data):
    """completeness is 0 (no missing dates) when every expected date is present."""
    metric = CompletenessMetric()
    result = metric.calculate(
        sample_data["raw"],
        None,
        {"threshold_ambar": 0.0001, "threshold_red": 0.20},
        **_params("age"),
        expected_dates=["2025-01-01"],
        date_column="information_date",
    )
    assert result.metric_name == "completeness"
    assert result.metric_value == pytest.approx(0.0, abs=1e-9)
    assert result.status == "GREEN"


def test_completeness_missing_window(sample_data):
    """completeness flags RED when expected observation windows are missing."""
    metric = CompletenessMetric()
    result = metric.calculate(
        sample_data["raw"],
        None,
        {"threshold_ambar": 0.0001, "threshold_red": 0.20},
        **_params("age"),
        expected_dates=["2025-01-01", "2024-12-01"],
        date_column="information_date",
    )
    # Only "2025-01-01" is present in the sample data; 1/2 windows missing.
    assert result.metric_value == pytest.approx(0.5, abs=0.01)
    assert result.status == "RED"
