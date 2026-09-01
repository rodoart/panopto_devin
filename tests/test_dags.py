"""Tests for Airflow DAG definitions."""

import importlib.util
import sys
from pathlib import Path

import pytest

DAG_FILES = [
    ("mecv_config_watcher", 4),
    ("mecv_production_runner", 2),
    ("mecv_alert_dispatcher", 2),
    ("mecv_output_validator", 3),
    ("mecv_orphan_cleanup", 1),
    ("mecv_calendar_loader", 3),
]


def _load_dag_module(name: str):
    """Load a DAG module from dags/<name>.py without requiring an __init__.py."""
    root = Path(__file__).resolve().parents[1]
    path = root / "dags" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # Ensure the repo root is on the path so the module can import ``mecv``.
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("dag_name, expected_tasks", DAG_FILES)
def test_dag_has_expected_tasks(dag_name, expected_tasks):
    """Each DAG exposes the correct dag_id and task count."""
    pytest.importorskip("airflow")
    module = _load_dag_module(dag_name)
    assert module.dag.dag_id == dag_name
    assert len(module.dag.tasks) == expected_tasks
