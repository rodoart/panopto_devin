"""DAG de Airflow panopto_alert_dispatcher; expone las funciones dispatch_alerts, handle_missing_data."""

from typing import Any
import dataclasses
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import pyspark.sql.functions as F

from panopto.alerts.aggregator import AggregateAlert
from panopto.alerts.dispatcher import EmailDispatcher
from panopto.calendar import BanamexCalendar
from panopto.config.schemas import OutputSchemas
from panopto.config.tables import PROCESS_CONFIG
from panopto.io.atomic_parquet_writer import AtomicParquetWriter
from panopto.logging import get_logger
from panopto.metrics.result import MetricResult
from panopto.sessions import SparkSessionBuilder

logger = get_logger(__name__)


def _load_metric_results(spark, model_id, information_date):
    """Carga resultados de metric_result_table como MetricResult."""
    df = spark.table(PROCESS_CONFIG.metric_result_table).filter(
        (F.col("model_id") == model_id) & (F.col("information_date") == information_date)
    )
    rows = [r.asDict() for r in df.collect()]
    fields = {f.name for f in dataclasses.fields(MetricResult)}
    return [MetricResult(**{k: r.get(k) for k in fields}) for r in rows]


def _load_aggregate_alerts(spark, model_id, information_date):
    """Carga agregación de alert_aggregate_table como AggregateAlert."""
    df = spark.table(PROCESS_CONFIG.alert_aggregate_table).filter(
        (F.col("model_id") == model_id) & (F.col("information_date") == information_date)
    )
    rows = [r.asDict() for r in df.collect()]
    fields = {f.name for f in dataclasses.fields(AggregateAlert)}
    result = []
    for r in rows:
        item = {k: r.get(k) for k in fields}
        item["red_equivalent"] = r.get("red_equivalent_used", 0)
        item["alert_ambar_pct"] = r.get("alert_ambar_pct_used", 0.0)
        item["alert_red_pct"] = r.get("alert_red_pct_used", 0.0)
        result.append(AggregateAlert(**item))
    return result


def _latest_execution_status(spark, model_id, information_date):
    """Devuelve el último status en execution_log_table para el modelo y fecha."""
    df = spark.table(PROCESS_CONFIG.execution_log_table).filter(
        (F.col("model_id") == model_id) & (F.col("information_date") == information_date)
    )
    row = df.orderBy(F.col("run_date").desc()).limit(1).collect()
    return row[0].status if row else None


def dispatch_alerts(**context: Any) -> None:
    """Función que envía alerts leyendo los resultados ya calculados."""
    from datetime import datetime as dt
    schemas = OutputSchemas()
    today = dt.fromisoformat(context["ds"]).date()
    execution_id = context["run_id"]
    spark = SparkSessionBuilder(app_name="panopto_alert_dispatcher").build()
    writer = AtomicParquetWriter(spark)
    dispatcher = EmailDispatcher(spark=spark)
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
        status = _latest_execution_status(spark, model_id, information_date)
        if status == "MISSING_DATA":
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
            email_df = spark.createDataFrame(
                schemas.normalize_rows(PROCESS_CONFIG.email_log_table, [email_row]),
                schema=schemas.get(PROCESS_CONFIG.email_log_table),
            )
            writer.write_atomic(email_df, PROCESS_CONFIG.email_log_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
            continue
        if status != "SUCCESS":
            logger.info(f"skipping {model_id}/{information_date}; latest execution status is {status}")
            continue
        try:
            metric_results = _load_metric_results(spark, model_id, information_date)
            aggregate_alerts = _load_aggregate_alerts(spark, model_id, information_date)
            if not metric_results:
                logger.info(f"no metric results for {model_id}/{information_date}; nothing to dispatch")
                continue
            log = dispatcher.dispatch(
                model_id=model_id,
                information_date=information_date,
                aggregate_alerts=aggregate_alerts,
                metric_results=metric_results,
                model_name=model_name,
                execution_id=execution_id,
            )
            email_row = dataclasses.asdict(log)
            email_row["information_date"] = information_date
            email_row["model_id"] = model_id
            email_df = spark.createDataFrame(
                schemas.normalize_rows(PROCESS_CONFIG.email_log_table, [email_row]),
                schema=schemas.get(PROCESS_CONFIG.email_log_table),
            )
            writer.write_atomic(email_df, PROCESS_CONFIG.email_log_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
        except Exception as exc:
            logger.error(f"alert dispatch failed for {model_id}: {exc}")
            raise exc


def handle_missing_data(**context: Any) -> None:
    """Función que gestiona missing data."""
    from panopto.sessions import PostgresSession
    today = datetime.fromisoformat(context["ds"]).date()
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
                logger.info(f"missing data for {row[0]}: {row[1]} days")


with DAG(
    "panopto_alert_dispatcher",
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
    dispatch = PythonOperator(task_id="dispatch_emails", python_callable=dispatch_alerts)
    missing = PythonOperator(task_id="handle_missing_data", python_callable=handle_missing_data)
    dispatch >> missing
