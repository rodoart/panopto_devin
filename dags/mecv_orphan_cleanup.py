from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mecv.logging import get_logger

logger = get_logger(__name__)


def cleanup_orphans(**context):
    from datetime import datetime as dt
    from mecv.sessions import SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="mecv_orphan_cleanup").build()
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = jvm.org.apache.hadoop.fs.Path
    staging = Path("/tmp/mecv_staging")
    if not fs.exists(staging):
        return
    cutoff = dt.now().timestamp() * 1000 - 7 * 24 * 60 * 60 * 1000
    for status in fs.listStatus(staging):
        if status.isDirectory() and status.getModificationTime() < cutoff:
            fs.delete(status.getPath(), True)
            logger.info(f"deleted {status.getPath().toString()}")


with DAG(
    "mecv_orphan_cleanup",
    default_args={
        "owner": "mecv",
        "start_date": datetime(2025, 10, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@weekly",
    catchup=False,
    tags=["mecv"],
) as dag:
    cleanup = PythonOperator(task_id="cleanup_orphans", python_callable=cleanup_orphans)
