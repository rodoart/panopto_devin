"""DAG de Airflow panopto_orphan_cleanup; expone las funciones cleanup_orphans."""

from typing import Any

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from panopto.config.tables import PROCESS_CONFIG
from panopto.logging import get_logger

logger = get_logger(__name__)


def cleanup_orphans(**context: Any) -> None:
    """Función que limpia orphans."""
    from datetime import datetime as dt
    from panopto.sessions import SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="panopto_orphan_cleanup").build()
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = jvm.org.apache.hadoop.fs.Path
    staging = Path(PROCESS_CONFIG.hdfs_staging_base)
    if not fs.exists(staging):
        return
    cutoff = dt.now().timestamp() * 1000 - 7 * 24 * 60 * 60 * 1000
    for status in fs.listStatus(staging):
        if status.isDirectory() and status.getModificationTime() < cutoff:
            path_str = status.getPath().toString().lower()
            if "checkpoint" in path_str:
                continue
            fs.delete(status.getPath(), True)
            logger.info(f"deleted {status.getPath().toString()}")


with DAG(
    "panopto_orphan_cleanup",
    default_args={
        "owner": "panopto",
        "start_date": datetime(2025, 10, 1),
        "retries": 10,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule="@weekly",
    catchup=False,
    tags=["panopto"],
) as dag:
    cleanup = PythonOperator(task_id="cleanup_orphans", python_callable=cleanup_orphans)
