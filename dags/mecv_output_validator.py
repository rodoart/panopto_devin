"""DAG de Airflow mecv_output_validator; expone las funciones validate_output_tables, trigger_tableau_refresh, log_refresh."""

from typing import Any

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


def validate_output_tables(**context: Any) -> None:
    """Función que valida output tables."""
    from mecv.sessions import SparkSessionBuilder
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"validating output tables for {today}")
    spark = SparkSessionBuilder(app_name="mecv_output_validator").build()
    metric_count = spark.sql(f"""
        SELECT count(*) AS c FROM {PROCESS_CONFIG.metric_result_table}
        WHERE information_date = '{today}'
    """).collect()[0]["c"]
    alert_count = spark.sql(f"""
        SELECT count(*) AS c FROM {PROCESS_CONFIG.alert_aggregate_table}
        WHERE information_date = '{today}'
    """).collect()[0]["c"]
    if metric_count == 0 or alert_count == 0:
        raise ValueError(f"missing output data for {today}: metrics={metric_count}, alerts={alert_count}")
    logger.info(f"validation ok for {today}: metrics={metric_count}, alerts={alert_count}")


def trigger_tableau_refresh(**context: Any) -> None:
    """Función que realiza la operación "trigger_tableau_refresh"."""
    pass


def log_refresh(**context: Any) -> None:
    """Función que registra refresh."""
    from mecv.sessions import SparkSessionBuilder
    from datetime import datetime as dt
    today = dt.now().strftime("%Y-%m-%d")
    spark = SparkSessionBuilder(app_name="mecv_output_validator_log").build()
    execution_id = context["run_id"]
    dag_id = context["dag"]["dag_id"]
    log_df = spark.createDataFrame([{
        "execution_id": execution_id,
        "dag_id": dag_id,
        "airflow_run_id": execution_id,
        "run_date": dt.now(),
        "end_date": dt.now(),
        "status": "SUCCESS",
        "error_message": "",
        "reason": "SCHEDULED",
        "variables_expected": 0,
        "variables_processed": 0,
        "variables_missing": 0,
        "metrics_calculated": 0,
        "metrics_failed": 0,
        "duration_seconds": 0,
        "information_date": today,
        "model_id": "__VALIDATOR__",
    }])
    from mecv.io.atomic_parquet_writer import AtomicParquetWriter
    writer = AtomicParquetWriter(spark)
    writer.write_atomic(log_df, PROCESS_CONFIG.execution_log_table, "__VALIDATOR__", today, execution_id, partition_cols=["information_date", "model_id"])


with DAG(
    "mecv_output_validator",
    default_args={
        "owner": "mecv",
        "start_date": datetime(2025, 10, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@daily",
    catchup=False,
    tags=["mecv"],
) as dag:
    validate = PythonOperator(task_id="validate_output_tables", python_callable=validate_output_tables)
    refresh = PythonOperator(task_id="trigger_tableau_refresh", python_callable=trigger_tableau_refresh)
    log = PythonOperator(task_id="log_refresh", python_callable=log_refresh)
    validate >> refresh >> log
