"""Tests for DataSourceSpec URI parsing."""

import pytest

from mecv.data.sources import DataSourceSpec


def test_hive_table_with_schema():
    """hive:schema.table sets source_type HIVE and splits schema/table."""
    spec = DataSourceSpec.from_metadata(
        source_table="hive:default.scores",
        source_column="score",
        information_date_column="information_date",
        partition_columns="[]",
    )
    assert spec.source_type == "HIVE"
    assert spec.schema == "default"
    assert spec.table_or_path == "scores"
    assert spec.column == "score"
    assert spec.information_date_column == "information_date"
    assert spec.partition_columns == []


def test_hive_table_without_schema():
    """hive:table_name works without a schema."""
    spec = DataSourceSpec.from_metadata(
        source_table="hive:scores",
        source_column="score",
        information_date_column="information_date",
        partition_columns="[]",
    )
    assert spec.source_type == "HIVE"
    assert spec.schema is None
    assert spec.table_or_path == "scores"


def test_parquet_path():
    """parquet:/path sets source_type PARQUET and keeps the path."""
    spec = DataSourceSpec.from_metadata(
        source_table="parquet:/data/scores.parquet",
        source_column="score",
        information_date_column="information_date",
        partition_columns='["dt"]',
    )
    assert spec.source_type == "PARQUET"
    assert spec.schema is None
    assert spec.table_or_path == "/data/scores.parquet"
    assert spec.partition_columns == ["dt"]


def test_default_source_type_is_hive():
    """Tables without a prefix are treated as HIVE."""
    spec = DataSourceSpec.from_metadata(
        source_table="my_schema.my_table",
        source_column="x",
        information_date_column="d",
        partition_columns="[]",
    )
    assert spec.source_type == "HIVE"
    assert spec.schema == "my_schema"
    assert spec.table_or_path == "my_table"
