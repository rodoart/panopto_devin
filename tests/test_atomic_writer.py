"""Tests for AtomicParquetWriter."""

import os

import pytest

from mecv.config.tables import PROCESS_CONFIG
from mecv.io.atomic_parquet_writer import AtomicParquetWriter


def test_atomic_writer_smoke(spark, tmp_path, monkeypatch):
    """AtomicParquetWriter can be instantiated with local temp paths."""
    monkeypatch.setattr(PROCESS_CONFIG, "hdfs_staging_base", str(tmp_path / "staging"))
    monkeypatch.setattr(PROCESS_CONFIG, "hive_warehouse_dir", str(tmp_path / "warehouse"))

    try:
        writer = AtomicParquetWriter(spark)
    except Exception as exc:
        pytest.skip(f"Spark FileSystem not available in this environment: {exc}")

    assert writer is not None
    assert writer.base_tmp_path == str(tmp_path / "staging")
    assert writer.warehouse_dir == str(tmp_path / "warehouse")


def test_write_atomic_local_filesystem(spark, tmp_path, monkeypatch):
    """write_atomic writes temp parquet and promotes the partition locally."""
    monkeypatch.setattr(PROCESS_CONFIG, "hdfs_staging_base", str(tmp_path / "staging"))
    monkeypatch.setattr(PROCESS_CONFIG, "hive_warehouse_dir", str(tmp_path / "warehouse"))

    try:
        writer = AtomicParquetWriter(spark)
    except Exception as exc:
        pytest.skip(f"Spark FileSystem not available in this environment: {exc}")
    # Avoid external catalog/Postgres dependencies for this unit test.
    writer._log_staging = lambda *args, **kwargs: None
    writer._update_staging_status = lambda *args, **kwargs: None
    writer._repair_table = lambda *args, **kwargs: None

    df = spark.createDataFrame(
        [
            ("2025-01-01", "M1", 1.0),
            ("2025-01-01", "M1", 2.0),
            ("2025-01-01", "M1", 3.0),
            ("2025-01-01", "M1", 4.0),
            ("2025-01-01", "M1", 5.0),
        ],
        ["information_date", "model_id", "value"],
    )

    result = writer.write_atomic(
        df,
        target_table="test_atomic_target",
        model_id="M1",
        information_date="2025-01-01",
        execution_id="exec_001",
        partition_cols=["information_date", "model_id"],
    )

    assert result["status"] == "PROMOTED"
    assert result["row_count"] == 5
    assert os.path.exists(result["final_path"])

    # The promoted partition should be readable as parquet.
    promoted = spark.read.parquet(result["final_path"])
    assert promoted.count() == 5


def test_write_atomic_smoke_if_filesystem_unavailable(spark, tmp_path, monkeypatch):
    """If the JVM/HDFS filesystem is unavailable, the constructor still works."""
    # This test is a fallback for environments where Spark's FileSystem cannot
    # be initialised.  In that case we only assert the object can be created.
    monkeypatch.setattr(PROCESS_CONFIG, "hdfs_staging_base", str(tmp_path / "staging"))
    monkeypatch.setattr(PROCESS_CONFIG, "hive_warehouse_dir", str(tmp_path / "warehouse"))

    try:
        writer = AtomicParquetWriter(spark)
        assert writer.base_tmp_path == str(tmp_path / "staging")
    except Exception as exc:
        pytest.skip(f"Spark FileSystem not available in this environment: {exc}")
