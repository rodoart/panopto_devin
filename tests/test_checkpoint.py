"""Tests for mecv.checkpoint."""

from mecv.checkpoint import Checkpoint


def test_checkpoint_compute_and_reuse(spark, tmp_path):
    """compute writes the first time and reads the cached parquet on subsequent calls."""
    calls = {"n": 0}

    def make_df():
        calls["n"] += 1
        return spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])

    checkpoint = Checkpoint(spark, str(tmp_path / "cp"))
    key = {"model_id": "M1", "date": "2025-01-01"}

    df1 = checkpoint.compute(key, make_df)
    assert df1.count() == 2
    assert calls["n"] == 1

    df2 = checkpoint.compute(key, make_df)
    assert df2.count() == 2
    assert calls["n"] == 1, "function should not be called when checkpoint exists"


def test_checkpoint_exists_after_write(spark, tmp_path):
    """exists returns True after a write and False before."""
    df = spark.createDataFrame([(1,)], ["id"])
    checkpoint = Checkpoint(spark, str(tmp_path / "cp"))
    key = {"k": "1"}
    assert not checkpoint.exists(key)
    checkpoint.write(df, key)
    assert checkpoint.exists(key)
