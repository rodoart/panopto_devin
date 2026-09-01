"""Tests for configuration loading."""

import os

import pytest

from mecv.config import Settings
from mecv.config.tables import ProcessConfig


def test_settings_from_env_defaults():
    """Settings.from_env() returns typed defaults from the environment."""
    settings = Settings.from_env()
    assert settings.env == "test"
    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 5432
    assert settings.smtp_port == 25
    assert settings.hdfs_staging_base == "/tmp/mecv_test_staging"


def test_settings_from_env_overrides(monkeypatch):
    """Environment variables override the Settings defaults."""
    monkeypatch.setenv("MECV_ENV", "prod")
    monkeypatch.setenv("MECV_POSTGRES_HOST", "pg.example.com")
    monkeypatch.setenv("MECV_POSTGRES_PORT", "5433")
    monkeypatch.setenv("MECV_HDFS_STAGING_BASE", "/prod/staging")

    settings = Settings.from_env()
    assert settings.env == "prod"
    assert settings.postgres_host == "pg.example.com"
    assert settings.postgres_port == 5433
    assert settings.hdfs_staging_base == "/prod/staging"


def test_process_config_from_json_defaults(tmp_path):
    """ProcessConfig.from_json() uses class defaults for a missing file."""
    missing = tmp_path / "does_not_exist.json"
    cfg = ProcessConfig.from_json(str(missing))
    assert cfg.model_summary_table == "model_summary_csi_psi_d_t_d"
    assert cfg.hdfs_staging_base == os.getenv("MECV_HDFS_STAGING_BASE")


def test_process_config_from_json_values_and_env_override(tmp_path, monkeypatch):
    """JSON values are loaded and MECV_HDFS_* env variables override them."""
    json_path = tmp_path / "tables.json"
    json_path.write_text(
        '{"model_summary_table": "custom_summary", '
        '"hdfs_staging_base": "/json/staging", '
        '"hive_warehouse_dir": "/json/warehouse"}'
    )
    monkeypatch.setenv("MECV_HDFS_STAGING_BASE", "/env/staging")
    monkeypatch.setenv("MECV_HIVE_WAREHOUSE_DIR", "/env/warehouse")

    cfg = ProcessConfig.from_json(str(json_path))
    assert cfg.model_summary_table == "custom_summary"
    assert cfg.hdfs_staging_base == "/env/staging"
    assert cfg.hive_warehouse_dir == "/env/warehouse"
