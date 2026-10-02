"""Módulo config con la(s) clase(s) Settings."""

import os
from typing import Optional

__path__ = [os.path.join(os.path.dirname(__file__), "config")]

from dataclasses import dataclass


def load_env_file(path: Optional[str] = None) -> bool:
    """Carga un archivo ``KEY=VALUE`` en ``os.environ`` sin sobreescribir lo ya definido.

    Orden de búsqueda: ``path`` explícito, ``PANOPTO_ENV_FILE``, y luego
    ``.env.local`` en la raíz del repo o en el directorio actual. En el
    cluster ninguno existe → no-op (la config llega por variables de entorno
    del DAG); en local permite que ``streamlit run`` funcione sin precargar
    ``.env.local`` en la shell.

    Debe llamarse ANTES de importar ``panopto.config.tables`` (que lee
    ``PANOPTO_TABLES_JSON`` al cargarse).
    """
    from pathlib import Path

    candidates = []
    if path:
        candidates.append(Path(path))
    elif os.getenv("PANOPTO_ENV_FILE"):
        candidates.append(Path(os.environ["PANOPTO_ENV_FILE"]))
    else:
        repo_root = Path(__file__).resolve().parents[1]
        candidates.extend([repo_root / ".env.local", Path.cwd() / ".env.local"])
    for candidate in candidates:
        if not candidate.is_file():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return True
    return False


@dataclass
class Settings:
    """Clase de datos que representa Settings."""
    env: str
    hive_metastore_uris: str
    hive_warehouse_dir: str
    hive_database: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    hdfs_staging_base: str
    kinit_keytab: Optional[str]
    kinit_principal: Optional[str]

    @classmethod
    def from_env(cls) -> "Settings":
        """
        Carga la configuración desde variables de entorno.

        Variables:
            PANOPTO_ENV: ambiente de ejecución (dev, qa, prod).
            PANOPTO_HIVE_METASTORE_URIS: URI del metastore Hive (thrift://...).
            PANOPTO_HIVE_WAREHOUSE_DIR: ruta base del warehouse Hive en HDFS.
            PANOPTO_HIVE_DATABASE: base de datos por defecto en Hive.
            PANOPTO_POSTGRES_HOST: host de PostgreSQL.
            PANOPTO_POSTGRES_PORT: puerto de PostgreSQL.
            PANOPTO_POSTGRES_DB: nombre de la base de datos PostgreSQL.
            PANOPTO_POSTGRES_USER: usuario de PostgreSQL.
            PANOPTO_POSTGRES_PASSWORD: contraseña de PostgreSQL.
            PANOPTO_SMTP_HOST: servidor SMTP.
            PANOPTO_SMTP_PORT: puerto SMTP.
            PANOPTO_SMTP_USER: usuario SMTP.
            PANOPTO_SMTP_PASSWORD: contraseña SMTP.
            PANOPTO_HDFS_STAGING_BASE: ruta HDFS para staging de parquet atómico.
            PANOPTO_KINIT_KEYTAB: ruta al keytab de Kerberos (opcional).
            PANOPTO_KINIT_PRINCIPAL: principal de Kerberos (opcional).
        """
        return cls(
            env=os.getenv("PANOPTO_ENV", "dev"),
            hive_metastore_uris=os.getenv("PANOPTO_HIVE_METASTORE_URIS", ""),
            hive_warehouse_dir=os.getenv("PANOPTO_HIVE_WAREHOUSE_DIR", "/user/hive/warehouse"),
            hive_database=os.getenv("PANOPTO_HIVE_DATABASE", "default"),
            postgres_host=os.getenv("PANOPTO_POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("PANOPTO_POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("PANOPTO_POSTGRES_DB", "panopto"),
            postgres_user=os.getenv("PANOPTO_POSTGRES_USER", "panopto_user"),
            postgres_password=os.getenv("PANOPTO_POSTGRES_PASSWORD", "CHANGEME"),
            smtp_host=os.getenv("PANOPTO_SMTP_HOST", "smtp.example.com"),
            smtp_port=int(os.getenv("PANOPTO_SMTP_PORT", "587")),
            smtp_user=os.getenv("PANOPTO_SMTP_USER", "alerts@example.com"),
            smtp_password=os.getenv("PANOPTO_SMTP_PASSWORD", "CHANGEME"),
            hdfs_staging_base=os.getenv("PANOPTO_HDFS_STAGING_BASE", "/tmp/panopto/staging"),
            kinit_keytab=os.getenv("PANOPTO_KINIT_KEYTAB"),
            kinit_principal=os.getenv("PANOPTO_KINIT_PRINCIPAL"),
        )
