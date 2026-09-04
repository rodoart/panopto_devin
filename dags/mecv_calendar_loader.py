"""DAG de Airflow mecv_calendar_loader; expone las funciones external_calendar_ready, convert_external_to_hive, sync_hive_to_postgres, pause_dependent_dags, send_red_alert, calendar_load_failure."""

from typing import Any

import calendar as cal
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


def external_calendar_ready(**context: Any) -> bool:
    """Función que realiza la operación "external_calendar_ready"."""
    from mecv.sessions import SparkSessionBuilder

    ds = context["ds"]
    year = int(ds[:4])
    expected_days = 366 if cal.isleap(year) else 365
    logger.info(f"checking external calendar for year {year}, expecting >= {expected_days} rows")
    external_table = PROCESS_CONFIG.external_banamex_calendar_table
    spark = SparkSessionBuilder(app_name="mecv_calendar_loader_check").build()
    try:
        count = spark.sql(f"""
            SELECT count(*) AS c FROM {external_table}
            WHERE calendar_date >= '{year}-01-01' AND calendar_date <= '{year}-12-31'
        """).collect()[0]["c"]
    except Exception as exc:
        logger.warning(f"external calendar {external_table} not ready: {exc}")
        return False
    logger.info(f"external calendar has {count} rows for year {year}")
    return count >= expected_days


def convert_external_to_hive(**context: Any) -> None:
    """Función que convierte external to hive."""
    from mecv.sessions import SparkSessionBuilder

    ds = context["ds"]
    year = int(ds[:4])
    calendar_table = PROCESS_CONFIG.banamex_calendar_table
    external_table = PROCESS_CONFIG.external_banamex_calendar_table
    logger.info(f"converting external calendar to {calendar_table} for year {year}")
    spark = SparkSessionBuilder(app_name="mecv_calendar_loader_convert").build()
    spark.sql(f"""
        INSERT OVERWRITE TABLE {calendar_table}
        SELECT
            calendar_date,
            is_business_day,
            is_holiday,
            holiday_name,
            current_timestamp() AS sync_timestamp
        FROM {external_table}
        WHERE calendar_date >= '{year}-01-01' AND calendar_date <= '{year}-12-31'
    """)
    logger.info(f"{calendar_table} overwritten for year {year}")


def sync_hive_to_postgres(**context: Any) -> None:
    """Función que realiza la operación "sync_hive_to_postgres"."""
    from mecv.sessions import PostgresSession, SparkSessionBuilder

    ds = context["ds"]
    year = int(ds[:4])
    logger.info(f"syncing banamex calendar to postgres for year {year}")
    spark = SparkSessionBuilder(app_name="mecv_calendar_loader_sync").build()
    psql = PostgresSession()
    calendar_table = PROCESS_CONFIG.banamex_calendar_table
    sync_table = PROCESS_CONFIG.banamex_calendar_sync_table
    df = spark.sql(f"""
        SELECT calendar_date, is_business_day, is_holiday, holiday_name
        FROM {calendar_table}
        WHERE calendar_date >= '{year}-01-01' AND calendar_date <= '{year}-12-31'
    """)
    rows = [
        (r.calendar_date, r.is_business_day, r.is_holiday, r.holiday_name, datetime.now())
        for r in df.collect()
    ]
    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {sync_table}
                    (calendar_date, is_business_day, is_holiday, holiday_name, sync_timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (calendar_date)
                DO UPDATE SET
                    is_business_day = EXCLUDED.is_business_day,
                    is_holiday = EXCLUDED.is_holiday,
                    holiday_name = EXCLUDED.holiday_name,
                    sync_timestamp = EXCLUDED.sync_timestamp
            """,
                rows,
            )
            conn.commit()
            logger.info(f"synced {len(rows)} calendar rows to postgres for year {year}")


def pause_dependent_dags(context: Any) -> None:
    """Función que realiza la operación "pause_dependent_dags"."""
    from airflow import settings
    from airflow.models import DagModel

    session = settings.Session()
    try:
        current = context["dag"]["dag_id"]
        for dag_model in session.query(DagModel).all():
            if dag_model.dag_id != current and dag_model.dag_id.startswith("mecv_"):
                dag_model.is_paused = True
        session.commit()
    finally:
        session.close()


def calendar_load_failure(context: Any) -> None:
    """Función que realiza la operación "calendar_load_failure"."""
    logger.error("calendar load failed after max sensor timeout; pausing dependent DAGs")
    pause_dependent_dags(context)


with DAG(
    "mecv_calendar_loader",
    default_args={
        "owner": "mecv",
        "start_date": datetime(2025, 1, 1),
        "retries": 5,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": calendar_load_failure,
    },
    schedule="@yearly",
    catchup=False,
    tags=["mecv"],
) as dag:
    wait = PythonSensor(
        task_id="wait_for_external_calendar",
        python_callable=external_calendar_ready,
        poke_interval=timedelta(hours=1),
        timeout=timedelta(days=7),
        mode="reschedule",
        soft_fail=False,
    )
    convert = PythonOperator(task_id="convert_external_to_hive", python_callable=convert_external_to_hive)
    sync = PythonOperator(task_id="sync_hive_to_postgres", python_callable=sync_hive_to_postgres)
    wait >> convert >> sync
