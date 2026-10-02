"""DAG de Airflow panopto_conda_pack; expone la función ensure_conda_pack.

Replica, en forma programada, el chequeo del script de terminal que garantiza
que el ``conda pack`` del ambiente virtual exista en HDFS:

    if hdfs dfs -test -e "$PANOPTO_HDFS_VIEW_TAR_GZ"; then
        echo "ya existe"
    else
        rm -f "$PANOPTO_UNIX_VIEW_TAR_GZ"
        conda-pack -n $ENV_NAME -o "$PANOPTO_UNIX_VIEW_TAR_GZ"
        hdfs dfs -mkdir -p "$PANOPTO_HDFS_VIEW_TAR_GZ_PARENT_DIR"
        hdfs dfs -put -f "$PANOPTO_UNIX_VIEW_TAR_GZ" "$PANOPTO_HDFS_VIEW_TAR_GZ_PARENT_DIR"
        hdfs dfs -chmod -R o+rx "$PANOPTO_HDFS_VIEW_TAR_GZ_PARENT_DIR"
    fi

Variables de entorno (mismos nombres que el script de shell, para que conviva
con ``config/environment_vars.sh`` del workspace Unix):

- ``PANOPTO_HDFS_VIEW_TAR_GZ`` (requerida): ruta completa del tar.gz en HDFS.
- ``PANOPTO_UNIX_VIEW_TAR_GZ``: ruta local del tar.gz a generar
  (default ``/tmp/<env>.tar.gz``).
- ``PANOPTO_HDFS_VIEW_TAR_GZ_PARENT_DIR``: directorio padre en HDFS
  (default: ``dirname`` de ``PANOPTO_HDFS_VIEW_TAR_GZ``).
- ``PANOPTO_CONDA_ENV_NAME`` o ``ENV_NAME`` (requerida solo si falta el pack):
  nombre del ambiente conda a empaquetar.
"""

import os
import posixpath
import shutil
import subprocess
from datetime import datetime, timedelta
from typing import Any, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

from panopto.kerberos import refresh_ticket
from panopto.logging import get_logger

logger = get_logger(__name__)


def _env(*names: str, default: Optional[str] = None) -> Optional[str]:
    """Devuelve la primera variable de entorno definida y no vacía."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _run(cmd: List[str]) -> None:
    """Ejecuta un comando, loguea stdout/stderr y levanta error si falla."""
    logger.info(f"$ {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout and proc.stdout.strip():
        logger.info(proc.stdout.strip())
    if proc.stderr and proc.stderr.strip():
        logger.info(proc.stderr.strip())
    if proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")


def _hdfs_exists(hdfs_bin: str, path: str) -> bool:
    """Equivalente a ``hdfs dfs -test -e <path>`` (rc 0 = existe)."""
    proc = subprocess.run(
        [hdfs_bin, "dfs", "-test", "-e", path],
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def ensure_conda_pack(**context: Any) -> None:
    """Verifica que el conda-pack del ambiente exista en HDFS; si falta, lo genera y lo sube."""
    if not refresh_ticket():
        raise RuntimeError("kerberos ticket refresh failed; check PANOPTO_KINIT_* env vars")

    hdfs_bin = shutil.which("hdfs")
    if not hdfs_bin:
        raise RuntimeError("hdfs not found in PATH")

    hdfs_tar = _env("PANOPTO_HDFS_VIEW_TAR_GZ")
    if not hdfs_tar:
        raise RuntimeError("PANOPTO_HDFS_VIEW_TAR_GZ is not set")
    parent_dir = _env(
        "PANOPTO_HDFS_VIEW_TAR_GZ_PARENT_DIR",
        default=posixpath.dirname(hdfs_tar),
    )

    if _hdfs_exists(hdfs_bin, hdfs_tar):
        logger.info(f"conda pack already exists in HDFS: {hdfs_tar}")
        return

    logger.info(f"conda pack missing in HDFS ({hdfs_tar}); packing and uploading")
    env_name = _env("PANOPTO_CONDA_ENV_NAME", "ENV_NAME")
    if not env_name:
        raise RuntimeError("PANOPTO_CONDA_ENV_NAME/ENV_NAME is not set; cannot run conda-pack")
    conda_pack = shutil.which("conda-pack")
    if not conda_pack:
        raise RuntimeError("conda-pack not found in PATH")
    local_tar = _env("PANOPTO_UNIX_VIEW_TAR_GZ", default=f"/tmp/{env_name}.tar.gz")

    if os.path.exists(local_tar):
        os.remove(local_tar)
    _run([conda_pack, "-n", env_name, "-o", local_tar])
    _run([hdfs_bin, "dfs", "-mkdir", "-p", parent_dir])
    _run([hdfs_bin, "dfs", "-put", "-f", local_tar, parent_dir])
    _run([hdfs_bin, "dfs", "-chmod", "-R", "o+rx", parent_dir])

    if not _hdfs_exists(hdfs_bin, hdfs_tar):
        raise RuntimeError(f"conda pack still missing in HDFS after upload: {hdfs_tar}")
    logger.info(f"conda pack uploaded to HDFS: {hdfs_tar}")


with DAG(
    "panopto_conda_pack",
    default_args={
        "owner": "panopto",
        "start_date": datetime(2025, 10, 1),
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule=timedelta(days=3),
    catchup=False,
    tags=["panopto"],
) as dag:
    PythonOperator(task_id="ensure_conda_pack", python_callable=ensure_conda_pack)
