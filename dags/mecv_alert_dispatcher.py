"""DAG de Airflow mecv_alert_dispatcher; expone las funciones dispatch_alerts, handle_missing_data."""

from typing import Any

import dataclasses
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


def dispatch_alerts(**context: Any) -> None:
    """Función que envía alerts."""
    from datetime import datetime as dt
    from mecv.alerts.aggregator import AlertAggregator
    from mecv.alerts.dispatcher import EmailDispatcher
    from mecv.calendar import BanamexCalendar
    from mecv.data.reader import DataReader
    from mecv.io.atomic_parquet_writer import AtomicParquetWriter
    from mecv.metrics.runner import MetricRunner, MissingDataError
    from mecv.sessions import SparkSessionBuilder

    today = dt.now().date()
    today_str = today.strftime("%Y-%m-%d")
    execution_id = context["run_id"]
    spark = SparkSessionBuilder(app_name="mecv_alert_dispatcher").build()
    reader = DataReader(spark)
    writer = AtomicParquetWriter(spark)
    aggregator = AlertAggregator()
    dispatcher = EmailDispatcher()
    calendar = BanamexCalendar()
    model_summary_table = PROCESS_CONFIG.model_summary_table
    model_summary = spark.sql(f"""
        SELECT * FROM {model_summary_table}
        WHERE process_date = (SELECT max(process_date) FROM {model_summary_table})
          AND status = 'active'
    """)
    for row in model_summary.collect():
        model_id = str(row.model_id)
        model_name = str(row.model_name)
        frequency = row.get("frequency", "daily")
        information_date = calendar.expected_information_date(frequency, today)
        try:
            baseline_days = calendar.previous_business_days(information_date, 2)
            baseline_date = baseline_days[0].isoformat() if baseline_days else None
            runner = MetricRunner(spark, reader, join_keys=["customer_id"], calendar=calendar)
            results = runner.run(model_id, information_date, execution_id, baseline_date=baseline_date)
            alerts = aggregator.aggregate(results)
            log = dispatcher.dispatch(
                model_id=model_id,
                information_date=information_date,
                aggregate_alerts=alerts,
                metric_results=results,
                model_name=model_name,
                execution_id=execution_id,
            )
            email_row = dataclasses.asdict(log)
            email_row["information_date"] = information_date
            email_row["model_id"] = model_id
            email_df = spark.createDataFrame([email_row])
            writer.write_atomic(email_df, PROCESS_CONFIG.email_log_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
        except MissingDataError:
            log = dispatcher.dispatch(
                model_id=model_id,
                information_date=information_date,
                aggregate_alerts=[],
                metric_results=[],
                model_name=model_name,
                missing_data=True,
                missing_days=1,
                execution_id=execution_id,
            )
            email_row = dataclasses.asdict(log)
            email_row["information_date"] = information_date
            email_row["model_id"] = model_id
            email_df = spark.createDataFrame([email_row])
            writer.write_atomic(email_df, PROCESS_CONFIG.email_log_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
        except Exception as exc:
            logger.error(f"alert dispatch failed for {model_id}: {exc}")
            raise exc


def handle_missing_data(**context: Any) -> None:
    """Función que gestiona missing data."""
    from mecv.sessions import PostgresSession
    today = datetime.now().date()
    psql = PostgresSession()
    execution_log_table = PROCESS_CONFIG.execution_log_table
    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT model_id, count(*) AS missing_days
                FROM {execution_log_table}
                WHERE information_date >= %s - interval '7 days'
                  AND status = 'MISSING_DATA'
                GROUP BY model_id
            """, (today,))
            for row in cur.fetchall():
                # enviar alerta de datos faltantes pendientes
                logger.info(f"missing data for {row[0]}: {row[1]} days")


with DAG(
    "mecv_alert_dispatcher",
    default_args={
        "owner": "mecv",
        "start_date": datetime(2025, 10, 1),
        "retries": 10,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule="@daily",
    catchup=False,
    tags=["mecv"],
) as dag:
    dispatch = PythonOperator(task_id="dispatch_emails", python_callable=dispatch_alerts)
    missing = PythonOperator(task_id="handle_missing_data", python_callable=handle_missing_data)
    dispatch >> missing
