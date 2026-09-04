"""DAG de Airflow mecv_config_watcher; expone las funciones sync_calendar, detect_config_changes, mode_training, validate_training."""

from typing import Any

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


def sync_calendar() -> None:
    """Función que realiza la operación "sync_calendar"."""
    from mecv.sessions import PostgresSession, SparkSessionBuilder
    spark = SparkSessionBuilder(app_name="mecv_config_watcher_sync").build()
    today = datetime.now().date()
    thirty_days_ago = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    calendar_table = PROCESS_CONFIG.banamex_calendar_table
    sync_table = PROCESS_CONFIG.banamex_calendar_sync_table
    df = spark.sql(f"""
        SELECT * FROM {calendar_table}
        WHERE calendar_date >= '{thirty_days_ago}'
    """)
    rows = df.collect()
    psql = PostgresSession()
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
                [
                    (r.calendar_date, r.is_business_day, r.is_holiday, r.holiday_name, datetime.now())
                    for r in rows
                ],
            )
            conn.commit()


def detect_config_changes(**context: Any) -> None:
    """Función que realiza la operación "detect_config_changes"."""
    from mecv.sessions import SparkSessionBuilder
    spark = SparkSessionBuilder(app_name="mecv_config_watcher_detect").build()
    today = datetime.now().strftime("%Y-%m-%d")
    model_summary_table = PROCESS_CONFIG.model_summary_table
    config_changelog_table = PROCESS_CONFIG.config_changelog_table
    latest = spark.sql(f"""
        SELECT * FROM {model_summary_table}
        WHERE process_date = (SELECT max(process_date) FROM {model_summary_table})
    """)
    existing = spark.sql(f"""
        SELECT model_id, max(process_date) AS max_date
        FROM {config_changelog_table}
        GROUP BY model_id
    """)
    latest.createOrReplaceTempView("latest_summary")
    existing.createOrReplaceTempView("existing_changes")
    new_models = spark.sql("""
        SELECT s.model_id
        FROM latest_summary s
        LEFT JOIN existing_changes e ON s.model_id = e.model_id
        WHERE e.model_id IS NULL
    """).collect()
    dag_id = context["dag"]["dag_id"]
    run_id = context["run_id"]
    for row in new_models:
        model_id = str(row.model_id)
        spark.sql(f"""
            INSERT INTO {config_changelog_table} VALUES (
                current_timestamp(),
                '{model_summary_table}',
                'NEW_MODEL',
                '',
                '',
                '',
                true,
                '',
                '{dag_id}',
                '{run_id}',
                '{today}',
                '{model_id}'
            )
        """)


def mode_training(**context: Any) -> None:
    """Función que realiza la operación "mode_training"."""
    from mecv.data.reader import DataReader
    from mecv.sessions import SparkSessionBuilder
    from mecv.training import TrainingMode
    spark = SparkSessionBuilder(app_name="mecv_config_watcher_train").build()
    reader = DataReader(spark)
    today = datetime.now().strftime("%Y-%m-%d")
    config_changelog_table = PROCESS_CONFIG.config_changelog_table
    df = spark.sql(f"""
        SELECT model_id FROM {config_changelog_table}
        WHERE process_date = '{today}'
          AND change_type = 'NEW_MODEL'
          AND triggered_retraining = true
    """)
    models = [str(r.model_id) for r in df.collect()]
    tm = TrainingMode(spark, reader)
    for model_id in models:
        try:
            tm.run(model_id, today, context["run_id"])
            logger.info(f"training ok for {model_id}")
        except Exception as exc:
            logger.error(f"training failed for {model_id}: {exc}")
            raise exc


def validate_training(**context: Any) -> None:
    """Función que valida training."""
    from airflow.exceptions import AirflowFailException
    from mecv.sessions import SparkSessionBuilder
    spark = SparkSessionBuilder(app_name="mecv_config_watcher_validate").build()
    today = datetime.now().strftime("%Y-%m-%d")
    config_changelog_table = PROCESS_CONFIG.config_changelog_table
    metric_threshold_auto_table = PROCESS_CONFIG.metric_threshold_auto_table
    df = spark.sql(f"""
        SELECT model_id FROM {config_changelog_table}
        WHERE process_date = '{today}'
          AND change_type = 'NEW_MODEL'
          AND triggered_retraining = true
    """)
    for row in df.collect():
        mid = str(row.model_id)
        count = spark.sql(f"""
            SELECT count(*) AS c FROM {metric_threshold_auto_table}
            WHERE process_date = '{today}' AND model_id = '{mid}'
        """).collect()[0]["c"]
        if count == 0:
            raise AirflowFailException(f"no training artifacts for {row.model_id}")


with DAG(
    "mecv_config_watcher",
    default_args={
        "owner": "mecv",
        "start_date": datetime(2025, 10, 1),
        "retries": 10,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=["mecv"],
) as dag:
    sync = PythonOperator(task_id="sync_calendar", python_callable=sync_calendar)
    detect = PythonOperator(task_id="detect_config_changes", python_callable=detect_config_changes)
    train = PythonOperator(task_id="mode_training", python_callable=mode_training)
    validate = PythonOperator(task_id="validate_training", python_callable=validate_training)
    sync >> detect >> train >> validate
