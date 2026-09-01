"""Tests for binning helpers."""

import pyspark.sql.functions as F
import pytest

from mecv.binning import categorical_bins, compute_bin_counts, compute_woe, numeric_bins


def test_numeric_bins_shape_and_bounds(spark, sample_data):
    """numeric_bins returns n numeric bins with -inf/inf edges."""
    df = sample_data["raw"]
    bins = numeric_bins(df, "age", n_bins=10)
    assert len(bins) == 10
    assert all(b["bin_type"] == "NUMERIC" for b in bins)
    assert bins[0]["lb"] == float("-inf")
    assert bins[-1]["ub"] == float("inf")
    assert all("lb" in b and "ub" in b for b in bins)


def test_categorical_bins_top_categories(spark, sample_data):
    """categorical_bins returns the top categories ordered by frequency."""
    df = sample_data["raw"]
    bins = categorical_bins(df, "category", top_n=10)
    assert len(bins) <= df.select("category").distinct().count()
    assert all(b["bin_type"] == "CATEGORICAL" for b in bins)
    assert all("category_value" in b for b in bins)
    # Top categories are A and B with 4 occurrences each (order of ties may vary).
    assert bins[0]["count_dev"] == 4
    assert bins[0]["category_value"] in {"A", "B"}


def test_compute_bin_counts(spark, sample_data):
    """compute_bin_counts populates count_dev/freq_dev for each numeric bin."""
    df = sample_data["raw"]
    bins = numeric_bins(df, "age", n_bins=10)
    bins = compute_bin_counts(df, "age", bins)

    total_non_null = df.filter(F.col("age").isNotNull()).count()
    assert sum(b["count_dev"] for b in bins) == total_non_null
    for b in bins:
        assert b["count_dev"] >= 0
        assert b["freq_dev"] == pytest.approx(b["count_dev"] / total_non_null)


def test_compute_woe():
    """compute_woe adds a woe key to each bin based on pos/neg counts."""
    bins = [
        {"count_pos": 10, "count_neg": 5},
        {"count_pos": 0, "count_neg": 20},
        {"count_pos": 30, "count_neg": 0},
    ]
    result = compute_woe(bins, positive=40, negative=25)
    assert all("woe" in b for b in result)
    assert result[0]["woe"] == pytest.approx(100.0 * ((10 + 1e-6) / 40) / ((5 + 1e-6) / 25))
