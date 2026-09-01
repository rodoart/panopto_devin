"""Tests for TrainingMode binning and threshold generation."""

from mecv.config.tables import PROCESS_CONFIG
from mecv.data.reader import DataReader
from mecv.training import TrainingMode


def _create_training_tables(spark, sample_data, model_id="M1"):
    """Create temp views needed by TrainingMode.run()."""
    spark.createDataFrame(
        [
            {
                "variable": "age",
                "var_type": "input",
                "data_type": "numeric",
                "source_table": "hive:training_raw",
                "source_column": "age",
                "information_date_column": "information_date",
                "partition_columns": "[]",
                "process_date": "2025-01-01",
                "model_id": model_id,
            },
            {
                "variable": "category",
                "var_type": "input",
                "data_type": "categorical",
                "source_table": "hive:training_raw",
                "source_column": "category",
                "information_date_column": "information_date",
                "partition_columns": "[]",
                "process_date": "2025-01-01",
                "model_id": model_id,
            },
        ]
    ).createOrReplaceTempView(PROCESS_CONFIG.variable_metadata_table)

    spark.createDataFrame(
        [
            {
                "variable": "category",
                "top_n_threshold": 50,
                "critical_top_k": 5,
                "process_date": "2025-01-01",
                "model_id": model_id,
            }
        ]
    ).createOrReplaceTempView(PROCESS_CONFIG.category_policy_table)

    sample_data["raw"].createOrReplaceTempView("training_raw")


class FakeAtomicWriter:
    """Captures the DataFrames passed to AtomicParquetWriter.write_atomic."""

    instances = []

    def __init__(self, spark):
        self.spark = spark
        self.calls = []
        FakeAtomicWriter.instances.append(self)

    def write_atomic(self, df, target_table, model_id, information_date, execution_id, partition_cols=None):
        self.calls.append((target_table, df))
        return {"status": "PROMOTED", "row_count": df.count()}


def test_training_auto_thresholds(spark, sample_data):
    """_auto_thresholds produces baseline values and thresholds for key metrics."""
    trainer = TrainingMode(spark, DataReader(spark))
    df = sample_data["raw"]
    rows = trainer._auto_thresholds(df, "age", "numeric", "input", sample_size=df.count())

    assert isinstance(rows, list)
    assert len(rows) >= 2
    metric_names = {r["metric_name"] for r in rows}
    assert "null_rate" in metric_names
    assert "outlier_rate" in metric_names
    assert all("threshold_ambar" in r for r in rows)
    assert all("threshold_red" in r for r in rows)


def test_training_run_writes_binned_data(spark, sample_data, monkeypatch):
    """run() returns True and writes CSI bins, category ranks and metric thresholds."""
    _create_training_tables(spark, sample_data)
    FakeAtomicWriter.instances.clear()
    monkeypatch.setattr("mecv.training.AtomicParquetWriter", FakeAtomicWriter)

    trainer = TrainingMode(spark, DataReader(spark))
    result = trainer.run("M1", "2025-01-01", "exec_001")

    assert result is True
    assert len(FakeAtomicWriter.instances) == 1
    writer = FakeAtomicWriter.instances[0]
    tables = {call[0] for call in writer.calls}
    assert PROCESS_CONFIG.csi_psi_table in tables
    assert PROCESS_CONFIG.metric_threshold_auto_table in tables
    assert PROCESS_CONFIG.category_baseline_rank_table in tables
