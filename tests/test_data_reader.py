"""Tests for DataReader filtering and projection."""

from pyspark.sql import SparkSession

from mecv.data.reader import DataReader
from mecv.data.sources import DataSourceSpec


def test_data_reader_filters_by_reading_dates(spark: SparkSession):
    """read() filters rows by information_date and projects required columns."""
    df = spark.createDataFrame(
        [
            ("2025-01-01", 1, 10.0),
            ("2025-01-02", 2, 20.0),
            ("2025-01-01", 3, 30.0),
            ("2025-01-03", 4, 40.0),
        ],
        ["information_date", "customer_id", "score"],
    )
    df.createOrReplaceTempView("scores")

    spec = DataSourceSpec.from_metadata(
        source_table="hive:scores",
        source_column="score",
        information_date_column="information_date",
        partition_columns="[]",
    )
    reader = DataReader(spark)

    result = reader.read(spec, "2025-01-01")
    assert result.count() == 2
    assert set(result.columns) == {"score", "information_date"}


def test_data_reader_multiple_dates_and_extra_columns(spark: SparkSession):
    """read() supports multiple reading dates and keeps requested extra columns."""
    df = spark.createDataFrame(
        [
            ("2025-01-01", 1, 10.0),
            ("2025-01-02", 2, 20.0),
            ("2025-01-03", 3, 30.0),
        ],
        ["information_date", "customer_id", "score"],
    )
    df.createOrReplaceTempView("scores")

    spec = DataSourceSpec.from_metadata(
        source_table="hive:scores",
        source_column="score",
        information_date_column="information_date",
        partition_columns="[]",
    )
    reader = DataReader(spark)

    result = reader.read(
        spec,
        ["2025-01-01", "2025-01-03"],
        extra_cols=["customer_id"],
    )
    assert result.count() == 2
    assert "score" in result.columns
    assert "information_date" in result.columns
    assert "customer_id" in result.columns


def test_data_reader_parquet_source(tmp_path, spark: SparkSession):
    """read() works with a PARQUET source path."""
    path = str(tmp_path / "scores.parquet")
    df = spark.createDataFrame(
        [
            ("2025-01-01", 1, 10.0),
            ("2025-01-02", 2, 20.0),
        ],
        ["information_date", "customer_id", "score"],
    )
    df.write.parquet(path)

    spec = DataSourceSpec.from_metadata(
        source_table=f"parquet:{path}",
        source_column="score",
        information_date_column="information_date",
        partition_columns="[]",
    )
    reader = DataReader(spark)
    result = reader.read(spec, "2025-01-01", extra_cols=["customer_id"])
    assert result.count() == 1
    assert set(result.columns) == {"score", "information_date", "customer_id"}
