"""Módulo sessions con la(s) clase(s) SparkSessionBuilder, PostgresSession."""

from typing import Any, Dict

import psycopg2
from pyspark.sql import SparkSession

from mecv.config import Settings
from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


class SparkSessionBuilder:
    """Clase que representa SparkSessionBuilder."""
    def __init__(self, app_name: str = "mecv", extra_conf: Dict[str, Any] = None) -> None:
        """Inicializa una nueva instancia de SparkSessionBuilder."""
        self.app_name = app_name
        self.extra_conf = extra_conf or {}

    def build(self) -> SparkSession:
        """Método que construye."""
        settings = Settings.from_env()
        builder = SparkSession.builder.appName(self.app_name)
        warehouse_dir = PROCESS_CONFIG.hive_warehouse_dir or settings.hive_warehouse_dir
        if settings.hive_metastore_uris:
            builder = (
                builder.config("spark.sql.catalogImplementation", "hive")
                .config("hive.metastore.uris", settings.hive_metastore_uris)
                .config("spark.sql.warehouse.dir", warehouse_dir)
            )
        for key, value in self.extra_conf.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()


class PostgresSession:
    """Clase que representa PostgresSession."""
    def __init__(self) -> None:
        """Inicializa una nueva instancia de PostgresSession."""
        self.settings = Settings.from_env()

    def connection(self) -> Any:
        """Método que realiza la operación "connection"."""
        return psycopg2.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            dbname=self.settings.postgres_db,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
        )

    def execute(self, query: str, params: tuple = None) -> None:
        """Método que realiza la operación "execute"."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
