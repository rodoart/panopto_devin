"""Shared pytest configuration and fixtures for MECV tests."""

import datetime as dt
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

# Set safe MECV_* defaults *before* importing any mecv modules, so tests can run
# in a local sandbox without a real Hive metastore or Postgres instance.
os.environ.setdefault("MECV_ENV", "test")
os.environ.setdefault("MECV_HIVE_METASTORE_URIS", "")
os.environ.setdefault("MECV_HIVE_WAREHOUSE_DIR", "/tmp/mecv_test_warehouse")
os.environ.setdefault("MECV_HIVE_DATABASE", "default")
os.environ.setdefault("MECV_POSTGRES_HOST", "localhost")
os.environ.setdefault("MECV_POSTGRES_PORT", "5432")
os.environ.setdefault("MECV_POSTGRES_DB", "mecv_test")
os.environ.setdefault("MECV_POSTGRES_USER", "mecv_test")
os.environ.setdefault("MECV_POSTGRES_PASSWORD", "mecv_test")
os.environ.setdefault("MECV_SMTP_HOST", "localhost")
os.environ.setdefault("MECV_SMTP_PORT", "25")
os.environ.setdefault("MECV_SMTP_USER", "test@example.com")
os.environ.setdefault("MECV_SMTP_PASSWORD", "test")
os.environ.setdefault("MECV_HDFS_STAGING_BASE", "/tmp/mecv_test_staging")
os.environ.setdefault("MECV_CHECKPOINT_BASE", "/tmp/mecv_test_checkpoints")
os.environ.setdefault("MECV_DISABLE_EMAILS", "true")
os.environ.setdefault(
    "MECV_EMAIL_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "config", "email_config.json"),
)

# Import mecv modules now that the environment is configured.  Importing
# mecv.metrics registers all metric subclasses.
from mecv.sessions import PostgresSession, SparkSessionBuilder  # noqa: E402
import mecv.metrics  # noqa: E402

from pyspark.sql import Row, SparkSession  # noqa: E402
import pyspark.sql.functions as F  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Build a local SparkSession for the whole test session."""
    import shutil
    checkpoint_base = os.environ.get("MECV_CHECKPOINT_BASE", "/tmp/mecv_test_checkpoints")
    if os.path.isdir(checkpoint_base):
        shutil.rmtree(checkpoint_base, ignore_errors=True)
    builder = SparkSessionBuilder(
        app_name="mecv-tests",
        extra_conf={
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.adaptive.enabled": "false",
        },
    )
    session = builder.build()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class _FakeCursor:
    """Cursor returned by the fake postgres connection."""

    def __init__(self, connection: "_FakeConnection") -> None:
        self.connection = connection
        self._last_query: Optional[str] = None
        self._last_params: Optional[tuple] = None

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        self._last_query = query
        self._last_params = params
        self.connection.last_query = query
        self.connection.last_params = params

    def fetchall(self) -> List[tuple]:
        return self.connection._match_results(self._last_query or "")

    def fetchone(self) -> Optional[tuple]:
        rows = self.fetchall()
        return rows[0] if rows else None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class _FakeConnection:
    """Connection returned by the fake postgres driver."""

    def __init__(self, factory: "FakePostgres") -> None:
        self.factory = factory
        self.last_query: Optional[str] = None
        self.last_params: Optional[tuple] = None
        self.cursor_instance = _FakeCursor(self)

    def _match_results(self, query: str) -> List[tuple]:
        for substring, rows in self.factory.query_results.items():
            if substring in query:
                return rows
        return self.factory.default_results

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        pass

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


@dataclass
class FakePostgres:
    """Configurable fake implementation of psycopg2.connect for unit tests."""

    query_results: Dict[str, List[tuple]]
    default_results: List[tuple]
    last_connect_kwargs: Optional[Dict[str, Any]]

    def __init__(self) -> None:
        self.query_results = {}
        self.default_results = []
        self.last_connect_kwargs = None

    def connect(self, **kwargs: Any) -> _FakeConnection:
        self.last_connect_kwargs = kwargs
        return _FakeConnection(self)

    def set_results(self, rows: List[tuple]) -> None:
        """Set the default result set returned for any query."""
        self.default_results = rows

    def add_query_results(self, substring: str, rows: List[tuple]) -> None:
        """Return ``rows`` when ``substring`` is found in the executed query."""
        self.query_results[substring] = rows


@pytest.fixture
def postgres_connection(monkeypatch) -> FakePostgres:
    """Patch ``psycopg2.connect`` so :class:`PostgresSession` returns a fake cursor."""
    fake = FakePostgres()
    monkeypatch.setattr("mecv.sessions.psycopg2.connect", fake.connect)
    yield fake


@pytest.fixture
def sample_data(spark: SparkSession) -> Dict[str, Any]:
    """Return a dictionary of small, deterministic Spark DataFrames for tests."""
    current_date = "2025-01-01"
    baseline_date = "2025-01-02"

    raw_current = spark.createDataFrame(
        [
            (1, current_date, 25.0, "A"),
            (2, current_date, 30.0, "B"),
            (3, current_date, 35.0, "A"),
            (4, current_date, 40.0, "A"),
            (5, current_date, 45.0, "B"),
            (6, current_date, None, "A"),  # null age for null_rate
            (7, current_date, 1000.0, "C"),  # outlier for outlier_rate
            (8, current_date, 28.0, "C"),
            (9, current_date, 32.0, "B"),
            (10, current_date, 29.0, "B"),
        ],
        ["customer_id", "information_date", "age", "category"],
    )

    # Baseline with a different distribution to make PSI/KS non-zero.
    raw_baseline = spark.createDataFrame(
        [
            (1, baseline_date, 22.0, "C"),
            (2, baseline_date, 27.0, "C"),
            (3, baseline_date, 33.0, "C"),
            (4, baseline_date, 38.0, "C"),
            (5, baseline_date, 41.0, "C"),
            (6, baseline_date, 44.0, "B"),
            (7, baseline_date, 29.0, "B"),
            (8, baseline_date, 31.0, "B"),
            (9, baseline_date, 36.0, "A"),
            (10, baseline_date, 39.0, "A"),
        ],
        ["customer_id", "information_date", "age", "category"],
    )

    score_current = spark.createDataFrame(
        [
            (1, current_date, 0.95),
            (2, current_date, 0.85),
            (3, current_date, 0.75),
            (4, current_date, 0.65),
            (5, current_date, 0.55),
            (6, current_date, 0.45),
            (7, current_date, 0.35),
            (8, current_date, 0.25),
            (9, current_date, 1.50),  # range violation (>1)
            (10, current_date, -0.20),  # range violation (<0)
        ],
        ["customer_id", "information_date", "score"],
    )

    score_baseline = spark.createDataFrame(
        [
            (1, baseline_date, 0.90),
            (2, baseline_date, 0.80),
            (3, baseline_date, 0.70),
            (4, baseline_date, 0.60),
            (5, baseline_date, 0.50),
            (6, baseline_date, 0.40),
            (7, baseline_date, 0.30),
            (8, baseline_date, 0.20),
            (9, baseline_date, 0.10),
            (10, baseline_date, 0.05),
        ],
        ["customer_id", "information_date", "score"],
    )

    target_current = spark.createDataFrame(
        [
            (1, current_date, 1),
            (2, current_date, 0),
            (3, current_date, 1),
            (4, current_date, 0),
            (5, current_date, 0),
            (6, current_date, 0),
            (7, current_date, 1),
            (8, current_date, 0),
            (9, current_date, 1),
            (10, current_date, 0),
        ],
        ["customer_id", "information_date", "target"],
    )

    target_baseline = spark.createDataFrame(
        [
            (1, baseline_date, 1),
            (2, baseline_date, 0),
            (3, baseline_date, 1),
            (4, baseline_date, 0),
            (5, baseline_date, 0),
            (6, baseline_date, 0),
            (7, baseline_date, 1),
            (8, baseline_date, 0),
            (9, baseline_date, 1),
            (10, baseline_date, 0),
        ],
        ["customer_id", "information_date", "target"],
    )

    joined_current = (
        score_current.select("customer_id", "score")
        .join(
            target_current.select("customer_id", "target"),
            on="customer_id",
            how="inner",
        )
    )

    joined_baseline = (
        score_baseline.select("customer_id", "score")
        .join(
            target_baseline.select("customer_id", "target"),
            on="customer_id",
            how="inner",
        )
    )

    return {
        "raw": raw_current,
        "raw_baseline": raw_baseline,
        "score": score_current,
        "score_baseline": score_baseline,
        "target": target_current,
        "target_baseline": target_baseline,
        "joined": joined_current,
        "joined_baseline": joined_baseline,
        "current_date": current_date,
        "baseline_date": baseline_date,
    }


@pytest.fixture
def checkpoint(spark: SparkSession, tmp_path):
    """Return a per-test Checkpoint instance backed by a temporary directory."""
    from mecv.checkpoint import Checkpoint
    base = str(tmp_path / "checkpoints")
    return Checkpoint(spark, base)
