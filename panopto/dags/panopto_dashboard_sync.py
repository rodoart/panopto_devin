"""DAG de Airflow panopto_dashboard_sync; expone la función sync_dashboard_tables.

Sin ``schedule``: solo corre cuando un DAG productor dispara un ``dag_run``
(``panopto_production_runner`` y ``panopto_output_validator`` lo hacen con
``TriggerDagRunOperator`` al terminar de escribir las tablas del tablero) o de
forma manual.

Acepta ``conf`` opcional para acotar la sincronización:

    airflow dags trigger panopto_dashboard_sync \
      --conf '{"information_date": "2026-08-07", "model_id": "1079_cta_lvl"}'

Sin ``conf`` hace un diff completo de particiones ``(information_date,
model_id)`` entre Hive y Postgres y repara lo que falte.
"""

from typing import Any

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from panopto.logging import get_logger

logger = get_logger(__name__)


def sync_dashboard_tables(**context: Any) -> None:
    """Sincroniza a PostgreSQL las particiones nuevas/modificadas de las tablas del dashboard."""
    from panopto.dashboard.pg_sync import sync_dashboard
    from panopto.sessions import PostgresSession, SparkSessionBuilder

    conf = (context.get("dag_run") and getattr(context["dag_run"], "conf", None)) or {}
    information_date = conf.get("information_date")
    model_id = conf.get("model_id")
    logger.info(f"dashboard sync triggered with conf={conf}")

    spark = SparkSessionBuilder(app_name="panopto_dashboard_sync").build()
    psql = PostgresSession()
    sync_dashboard(spark, psql, information_date=information_date, model_id=model_id)


with DAG(
    "panopto_dashboard_sync",
    default_args={
        "owner": "panopto",
        "start_date": datetime(2025, 10, 1),
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule=None,
    catchup=False,
    tags=["panopto"],
) as dag:
    PythonOperator(task_id="sync_dashboard_tables", python_callable=sync_dashboard_tables)
