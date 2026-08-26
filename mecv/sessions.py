from typing import Any, Dict

import psycopg2
from pyspark.sql import SparkSession

from mecv.config import Settings
from mecv.logging import get_logger

logger = get_logger(__name__)


class SparkSessionBuilder:
    def __init__(self, app_name: str = "mecv", extra_conf: Dict[str, Any] = None):
        self.app_name = app_name
        self.extra_conf = extra_conf or {}

    def build(self) -> SparkSession:
        settings = Settings.from_env()
        builder = SparkSession.builder.appName(self.app_name)
        if settings.hive_metastore_uris:
            builder = (
                builder.config("spark.sql.catalogImplementation", "hive")
                .config("hive.metastore.uris", settings.hive_metastore_uris)
                .config("spark.sql.warehouse.dir", settings.hive_warehouse_dir)
            )
        for key, value in self.extra_conf.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()


class PostgresSession:
    def __init__(self):
        self.settings = Settings.from_env()

    def connection(self):
        return psycopg2.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            dbname=self.settings.postgres_db,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
        )

    def execute(self, query: str, params: tuple = None):
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
