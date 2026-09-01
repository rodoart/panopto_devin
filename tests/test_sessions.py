"""Tests for Spark and Postgres session builders."""

from pyspark.sql import SparkSession

from mecv.sessions import PostgresSession, SparkSessionBuilder


def test_spark_session_builder_returns_spark_session(spark: SparkSession):
    """``SparkSessionBuilder.build()`` returns a SparkSession."""
    assert isinstance(spark, SparkSession)
    # The fixture already builds the session with the test app name.
    assert "mecv-tests" in spark.sparkContext.appName


def test_spark_session_builder_uses_config(spark: SparkSession):
    """The shared test session reflects the extra configuration passed to the builder."""
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"


def test_postgres_session_uses_expected_credentials(postgres_connection):
    """PostgresSession passes the configured credentials to psycopg2.connect."""
    psql = PostgresSession()
    with psql.connection() as _conn:
        pass

    kwargs = postgres_connection.last_connect_kwargs
    assert kwargs is not None
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == "5432"
    assert kwargs["dbname"] == "mecv_test"
    assert kwargs["user"] == "mecv_test"
    assert kwargs["password"] == "mecv_test"
