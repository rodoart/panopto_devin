"""Módulo config con la(s) clase(s) Settings."""

import os

__path__ = [os.path.join(os.path.dirname(__file__), "config")]

from dataclasses import dataclass


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
        )
