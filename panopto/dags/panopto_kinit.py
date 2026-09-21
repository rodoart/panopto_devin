"""DAG de Airflow panopto_kinit; refresca el ticket Kerberos periódicamente."""

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from panopto.kerberos import refresh_ticket
from panopto.logging import get_logger

logger = get_logger(__name__)


def kinit_refresh(**context: Any) -> None:
    """Renueva el ticket Kerberos usando el keytab configurado."""
    ok = refresh_ticket()
    if not ok:
        raise RuntimeError("kerberos ticket refresh failed; check keytab and PANOPTO_KINIT_* env vars")
    logger.info("scheduled kinit refresh completed")


with DAG(
    "panopto_kinit",
    default_args={
        "owner": "panopto",
        "start_date": datetime(2025, 10, 1),
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule=timedelta(hours=7),
    catchup=False,
    tags=["panopto"],
) as dag:
    PythonOperator(task_id="kinit_refresh", python_callable=kinit_refresh)
