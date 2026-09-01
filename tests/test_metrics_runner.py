"""Tests for MetricRunner orchestration."""

import datetime as dt
from typing import Any, List, Optional

import pytest
import pyspark.sql.functions as F
from pyspark.sql import Row, SparkSession

from mecv.binning import numeric_bins
from mecv.config.tables import PROCESS_CONFIG
from mecv.data.reader import DataReader
from mecv.metrics.result import MetricResult
from mecv.metrics.runner import MetricRunner, MissingDataError


def _create_mock_tables(spark: SparkSession, sample_data: dict, model_id: str = "M1") -> None:
    """Create temp views that mimic the production configuration tables."""
    # Source tables contain both current and baseline dates.
    # Use per-table information date aliases so conjugate joins avoid duplicate columns.
    score_src = (
        sample_data["score"]
        .union(sample_data["score_baseline"])
        .withColumn("info_date_score", F.col("information_date"))
    )
    target_src = (
        sample_data["target"]
        .union(sample_data["target_baseline"])
        .withColumn("info_date_target", F.col("information_date"))
    )
    raw_src = sample_data["raw"].union(sample_data["raw_baseline"])
    score_src.createOrReplaceTempView("scores")
    target_src.createOrReplaceTempView("targets")
    raw_src.createOrReplaceTempView("raw")

    # model_summary: model metadata and cutoff.
    spark.createDataFrame(
        [Row(model_id=model_id, process_date="2025-01-01", cut_off_probability=0.5, frequency="daily")]
    ).createOrReplaceTempView(PROCESS_CONFIG.model_summary_table)

    # variable_metadata: variables to process.
    metadata = [
        Row(
            variable="score",
            var_type="score",
            data_type="numeric",
            reading_mode="each",
            information_date_column="info_date_score",
            source_table="hive:scores",
            source_column="score",
            partition_columns="[]",
            process_date="2025-01-01",
            model_id=model_id,
        ),
        Row(
            variable="target",
            var_type="target",
            data_type="numeric",
            reading_mode="each",
            information_date_column="info_date_target",
            source_table="hive:targets",
            source_column="target",
            partition_columns="[]",
            process_date="2025-01-01",
            model_id=model_id,
        ),
        Row(
            variable="age",
            var_type="input",
            data_type="numeric",
            reading_mode="each",
            information_date_column="information_date",
            source_table="hive:raw",
            source_column="age",
            partition_columns="[]",
            process_date="2025-01-01",
            model_id=model_id,
        ),
        Row(
            variable="category",
            var_type="input",
            data_type="categorical",
            reading_mode="each",
            information_date_column="information_date",
            source_table="hive:raw",
            source_column="category",
            partition_columns="[]",
            process_date="2025-01-01",
            model_id=model_id,
        ),
    ]
    spark.createDataFrame(metadata).createOrReplaceTempView(PROCESS_CONFIG.variable_metadata_table)

    # Bins for score used by psi_approved / psi_rejected / psi_canonical.
    score_bins = numeric_bins(sample_data["score_baseline"], "score", n_bins=10)
    csi_rows = []
    for b in score_bins:
        row = dict(b)
        row.update({"variable": "score", "type": "score", "process_date": "2025-01-01", "model_id": model_id})
        csi_rows.append(row)
    spark.createDataFrame(csi_rows).createOrReplaceTempView(PROCESS_CONFIG.csi_psi_table)

    # Category policy for the composition drift top-N.
    spark.createDataFrame(
        [
            Row(
                variable="category",
                top_n_threshold=2,
                critical_top_k=5,
                process_date="2025-01-01",
                model_id=model_id,
            )
        ]
    ).createOrReplaceTempView(PROCESS_CONFIG.category_policy_table)

    # Empty-ish thresholds and metric_threshold_auto tables for this model.
    spark.createDataFrame(
        [Row(model_id="other", process_date="2025-01-01")],
        "model_id STRING, process_date STRING",
    ).createOrReplaceTempView(PROCESS_CONFIG.thresholds_table)

    spark.createDataFrame(
        [Row(model_id="other", process_date="2025-01-01")],
        "model_id STRING, process_date STRING",
    ).createOrReplaceTempView(PROCESS_CONFIG.metric_threshold_auto_table)


class FakeCalendar:
    """Calendar stub used to exercise date resolution without Postgres."""

    def previous_business_days(self, calendar_date: Any, n: int = 1) -> List[dt.date]:
        d = dt.date.fromisoformat(calendar_date) if isinstance(calendar_date, str) else calendar_date
        return [d - dt.timedelta(days=i + 1) for i in range(n)]

    def first_business_day_of_period(self, calendar_date: Any, period: str) -> str:
        return "2025-01-02"

    def last_business_day_of_period(self, calendar_date: Any, period: str) -> str:
        return "2025-01-30"


def test_metric_runner_period_dates(spark: SparkSession):
    """_period_dates maps reading_mode to the expected date list."""
    runner = MetricRunner(spark, DataReader(spark), calendar=FakeCalendar())

    assert runner._period_dates("2025-01-15", "each", "daily") == ["2025-01-15"]
    assert runner._period_dates("2025-01-15", "first", "monthly") == ["2025-01-02"]
    assert runner._period_dates("2025-01-15", "last", "monthly") == ["2025-01-30"]


def test_metric_runner_resolve_dates_with_baseline(spark: SparkSession):
    """_resolve_dates honors an explicit baseline_date."""
    runner = MetricRunner(spark, DataReader(spark), calendar=FakeCalendar())
    current, baseline = runner._resolve_dates("2025-01-15", "2025-01-10", "each", "daily")
    assert current == ["2025-01-15"]
    assert baseline == ["2025-01-10"]


def test_metric_runner_resolve_dates_without_baseline(spark: SparkSession):
    """_resolve_dates falls back to the previous business day."""
    runner = MetricRunner(spark, DataReader(spark), calendar=FakeCalendar())
    current, baseline = runner._resolve_dates("2025-01-15", None, "each", "daily")
    assert current == ["2025-01-15"]
    assert baseline == ["2025-01-14"]


def test_metric_runner_resolve_dates_first_monthly(spark: SparkSession):
    """_resolve_dates for first/last modes uses the previous period."""
    runner = MetricRunner(spark, DataReader(spark), calendar=FakeCalendar())
    current, baseline = runner._resolve_dates("2025-01-15", None, "first", "monthly")
    assert current == ["2025-01-02"]
    # Previous period is 2024-12-15, fake calendar returns fixed string.
    assert baseline == ["2025-01-02"]


def test_metric_runner_run_returns_metric_results(spark: SparkSession, sample_data: dict):
    """run() loads config tables and returns a list of MetricResult objects."""
    _create_mock_tables(spark, sample_data)
    runner = MetricRunner(spark, DataReader(spark), join_keys=["customer_id"])

    results = runner.run(
        model_id="M1",
        information_date="2025-01-01",
        execution_id="exec_001",
        baseline_date="2025-01-02",
    )

    assert isinstance(results, list)
    assert len(results) > 0
    assert all(isinstance(r, MetricResult) for r in results)
    metric_names = {r.metric_name for r in results}
    assert "null_rate" in metric_names
    assert "range_violation" in metric_names


def test_metric_runner_missing_data_raises(spark: SparkSession, sample_data: dict):
    """run() raises MissingDataError when no rows match the reading date."""
    _create_mock_tables(spark, sample_data)
    runner = MetricRunner(spark, DataReader(spark), join_keys=["customer_id"])

    with pytest.raises(MissingDataError):  # noqa: F821
        runner.run(
            model_id="M1",
            information_date="2099-01-01",
            execution_id="exec_001",
            baseline_date="2025-01-02",
        )
