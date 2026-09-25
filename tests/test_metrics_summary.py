"""Tests for ScoringSummaryBuilder (canonical Summary Scoring Monitoring row)."""

from datetime import datetime

from panopto.metrics.result import MetricResult
from panopto.metrics.summary import ScoringSummaryBuilder


def _result(var_type, metric_name, value, status, baseline=None, variable="v") -> MetricResult:
    return MetricResult(
        model_id="M1",
        information_date="2025-01-15",
        variable=variable,
        var_type=var_type,
        metric_name=metric_name,
        metric_value=value,
        baseline_value=baseline,
        threshold_ambar=0.10,
        threshold_red=0.20,
        status=status,
        run_date=datetime.now(),
    )


def test_scoring_summary_all_green_is_ok():
    """When every metric is GREEN, control checks pass and General Status is OK."""
    results = [
        _result("score", "psi_canonical", 0.05, "GREEN"),
        _result("score", "psi_dynamic", 0.02, "GREEN"),
        _result("raw", "psi_canonical", 0.08, "GREEN", variable="age"),
        _result("raw", "median_shift", 0.01, "GREEN", variable="age"),
        _result("raw", "population_variation", 0.01, "GREEN", variable="age"),
        _result("raw", "completeness", 0.0, "GREEN", variable="age"),
    ]
    summaries = [
        {"var_type": "score", "statistic": "count_total", "statistic_value": 1000.0},
    ]

    row = ScoringSummaryBuilder.build(
        model_id="M1",
        model_name="Model One",
        information_date="2025-01-15",
        vintage="2025-01-01",
        results=results,
        summaries=summaries,
        data_availability_done=True,
    )

    assert row["model_id"] == "M1"
    assert row["control_data_availability"] == "DONE"
    assert row["control_pre_scoring"] == "OK"
    assert row["psi"] == 0.05
    assert row["psi_variation"] == 0.02
    assert row["csi_max"] == 0.08
    assert row["csi_status"] == "OK"
    assert row["population_scored"] == 1000.0
    assert row["general_status"] == "OK"


def test_scoring_summary_csi_stop_triggers_warning():
    """A RED psi_canonical on a raw/input variable stops CSI and sets General Status to WARNING."""
    results = [
        _result("score", "psi_canonical", 0.05, "GREEN"),
        _result("raw", "psi_canonical", 0.35, "RED", variable="age"),
    ]
    row = ScoringSummaryBuilder.build(
        model_id="M1",
        model_name="Model One",
        information_date="2025-01-15",
        vintage="2025-01-01",
        results=results,
        summaries=[],
        data_availability_done=True,
    )
    assert row["csi_status"] == "Stop"
    assert row["general_status"] == "WARNING"


def test_scoring_summary_pre_scoring_reprocess():
    """A RED pre-scoring check (median_shift/population_variation/completeness) forces Reprocess."""
    results = [
        _result("raw", "median_shift", 0.5, "RED", variable="age"),
    ]
    row = ScoringSummaryBuilder.build(
        model_id="M1",
        model_name="Model One",
        information_date="2025-01-15",
        vintage="2025-01-01",
        results=results,
        summaries=[],
        data_availability_done=True,
    )
    assert row["control_pre_scoring"] == "Reprocess"
    assert row["general_status"] == "Reprocess"


def test_scoring_summary_missing_data_availability():
    """When data availability is not done, General Status is DQR Process Pending."""
    row = ScoringSummaryBuilder.build(
        model_id="M1",
        model_name="Model One",
        information_date="2025-01-15",
        vintage="2025-01-01",
        results=[],
        summaries=[],
        data_availability_done=False,
    )
    assert row["control_data_availability"] == "PENDING"
    assert row["general_status"] == "DQR Process Pending"
