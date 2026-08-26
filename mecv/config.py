import os
from dataclasses import dataclass


@dataclass
class Settings:
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
    def from_env(cls):
        return cls(
            env=os.getenv("MECV_ENV", "dev"),
            hive_metastore_uris=os.getenv("MECV_HIVE_METASTORE_URIS", ""),
            hive_warehouse_dir=os.getenv("MECV_HIVE_WAREHOUSE_DIR", "/user/hive/warehouse"),
            hive_database=os.getenv("MECV_HIVE_DATABASE", "default"),
            postgres_host=os.getenv("MECV_POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("MECV_POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("MECV_POSTGRES_DB", "mecv"),
            postgres_user=os.getenv("MECV_POSTGRES_USER", "mecv_user"),
            postgres_password=os.getenv("MECV_POSTGRES_PASSWORD", "CHANGEME"),
            smtp_host=os.getenv("MECV_SMTP_HOST", "smtp.example.com"),
            smtp_port=int(os.getenv("MECV_SMTP_PORT", "587")),
            smtp_user=os.getenv("MECV_SMTP_USER", "alerts@example.com"),
            smtp_password=os.getenv("MECV_SMTP_PASSWORD", "CHANGEME"),
            hdfs_staging_base=os.getenv("MECV_HDFS_STAGING_BASE", "/tmp/mecv/staging"),
        )
