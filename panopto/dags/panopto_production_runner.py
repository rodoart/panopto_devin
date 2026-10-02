"""DAG de Airflow panopto_production_runner; expone las funciones run_production."""

from typing import Any

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from panopto.config.tables import PROCESS_CONFIG
from panopto.logging import get_logger

logger = get_logger(__name__)


def run_production(**context: Any) -> None:
    """Función que ejecuta production."""
    from datetime import datetime as dt
    from panopto.alerts.aggregator import AlertAggregator
    from panopto.calendar import BanamexCalendar
    from panopto.data.reader import DataReader
    from panopto.config.schemas import OutputSchemas
    from panopto.io.atomic_parquet_writer import AtomicParquetWriter
    from panopto.production import has_successful_run, run_model_date
    from panopto.sessions import PostgresSession, SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="panopto_production_runner").build()
    schemas = OutputSchemas()
    reader = DataReader(spark)
    psql = PostgresSession()
    writer = AtomicParquetWriter(spark)
    aggregator = AlertAggregator()
    calendar = BanamexCalendar()
    today = dt.fromisoformat(context["ds"]).date()
    today_str = context["ds"]
    execution_id = context["run_id"]
    logger.info(f"starting production run for {today_str}")
    dag_id = context["dag"]["dag_id"]

    calendar_sync_table = PROCESS_CONFIG.banamex_calendar_sync_table
    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT is_business_day FROM {calendar_sync_table} WHERE calendar_date = %s", (today,))
            row = cur.fetchone()
            is_business = row[0] if row else True

    model_summary_table = PROCESS_CONFIG.model_summary_table
    model_summary = spark.sql(f"""
        SELECT * FROM {model_summary_table}
        WHERE process_date = (SELECT max(process_date) FROM {model_summary_table})
          AND status = 'active'
    """)
    models = [r.asDict() for r in model_summary.collect()]

    for model in models:
        model_id = str(model["model_id"])
        frequency = model.get("frequency", "daily")
        execution_monthly_day = model.get("execution_monthly_day")
        execution_weekday = model.get("execution_weekday")
        information_date = calendar.expected_information_date(
            frequency,
            today,
            execution_monthly_day=execution_monthly_day,
            execution_weekday=execution_weekday,
        )
        logger.info(f"processing model {model_id} with frequency {frequency}, information_date {information_date}")
        if frequency == "business_daily" and not is_business:
            continue
        if frequency in ("weekly", "monthly") and information_date != today_str:
            continue
        if has_successful_run(spark, model_id, information_date):
            logger.info(f"skipping {model_id}/{information_date}; successful run already recorded")
            continue
        run_model_date(
            spark, reader, writer, schemas, aggregator, calendar,
            model, information_date, execution_id, dag_id,
        )


with DAG(
    "panopto_production_runner",
    default_args={
        "owner": "panopto",
        "start_date": datetime(2025, 10, 1),
        "retries": 10,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule="@daily",
    catchup=False,
    tags=["panopto"],
) as dag:
    run = PythonOperator(task_id="run_production", python_callable=run_production)
    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_alert_dispatcher",
        trigger_dag_id="panopto_alert_dispatcher",
        conf={"information_date": "{{ ds }}"},
    )
    trigger_sync = TriggerDagRunOperator(
        task_id="trigger_dashboard_sync",
        trigger_dag_id="panopto_dashboard_sync",
        conf={"information_date": "{{ ds }}"},
    )
    run >> [trigger_alert, trigger_sync]
