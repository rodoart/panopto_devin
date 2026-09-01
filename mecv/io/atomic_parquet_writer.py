"""Módulo atomic_parquet_writer con la(s) clase(s) AtomicParquetWriter."""

import uuid
from datetime import datetime
from typing import Callable, List, Optional

from pyspark.sql import DataFrame, Row, SparkSession

from mecv.config import Settings
from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


class AtomicParquetWriter:
    """Clase que representa AtomicParquetWriter."""
    def __init__(self, spark: SparkSession, base_tmp_path: Optional[str] = None, control_table: Optional[str] = None) -> None:
        """Inicializa una nueva instancia de AtomicParquetWriter."""
        self.spark = spark
        settings = Settings.from_env()
        self.base_tmp_path = (base_tmp_path or PROCESS_CONFIG.hdfs_staging_base or settings.hdfs_staging_base).rstrip("/")
        self.control_table = control_table or PROCESS_CONFIG.staging_control_table
        self.warehouse_dir = (PROCESS_CONFIG.hive_warehouse_dir or settings.hive_warehouse_dir).rstrip("/")
        jvm = spark._jvm
        self.fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        self.Path = jvm.org.apache.hadoop.fs.Path
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    def write_atomic(
        self,
        df: DataFrame,
        target_table: str,
        model_id: str,
        information_date: str,
        execution_id: str,
        partition_cols: Optional[List[str]] = None,
        extra_validation: Optional[Callable[[DataFrame], None]] = None,
    ) -> dict:
        """Método que escribe atomic."""
        model_id = str(model_id)
        information_date = str(information_date)
        logger.info(f"writing {target_table} for {model_id}/{information_date}")
        staging_id = uuid.uuid4().hex
        partition_cols = partition_cols or ["information_date", "model_id"]
        temp_path = f"{self.base_tmp_path}/{target_table}/{information_date}/{model_id}_{execution_id}_{staging_id}"
        suffix = self._partition_suffix(partition_cols, information_date, model_id)
        source_path = f"{temp_path}/{suffix}" if suffix else temp_path
        final_path = (
            f"{self.warehouse_dir}/{target_table}/{suffix}"
            if suffix
            else f"{self.warehouse_dir}/{target_table}"
        )
        self._write_temp(df, temp_path, partition_cols)
        temp_df = self.spark.read.parquet(temp_path)
        row_count = temp_df.count()
        if row_count == 0:
            raise RuntimeError(f"staging {staging_id}: no rows written")
        if not temp_df.columns:
            raise RuntimeError(f"staging {staging_id}: empty schema")
        if extra_validation:
            extra_validation(temp_df)
        self._log_staging(
            staging_id,
            execution_id,
            model_id,
            target_table,
            information_date,
            temp_path,
            final_path,
            row_count,
            "VALIDATED",
        )
        self._promote(source_path, final_path)
        self._update_staging_status(staging_id, "PROMOTED", row_count)
        self._repair_table(target_table)
        return {
            "staging_id": staging_id,
            "temp_path": source_path,
            "final_path": final_path,
            "row_count": row_count,
            "status": "PROMOTED",
        }

    def _write_temp(self, df: DataFrame, temp_path: str, partition_cols: List[str]) -> None:
        """Helper interno que escribe temp."""
        writer = df.write.mode("overwrite").option("compression", "snappy")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(temp_path)

    def _partition_suffix(self, partition_cols: List[str], information_date: str, model_id: str) -> str:
        """Helper interno que realiza la operación "partition_suffix"."""
        parts = []
        for col in partition_cols:
            if col == "information_date":
                parts.append(f"information_date={information_date}")
            elif col == "process_date":
                parts.append(f"process_date={information_date}")
            elif col == "model_id":
                parts.append(f"model_id={model_id}")
            else:
                raise ValueError(f"unsupported partition column: {col}")
        return "/".join(parts)

    def _promote(self, source_path: str, final_path: str) -> None:
        """Helper interno que realiza la operación "promote"."""
        final_jvm = self.Path(final_path)
        if self.fs.exists(final_jvm):
            self.fs.delete(final_jvm, True)
        source_jvm = self.Path(source_path)
        if not self.fs.exists(source_jvm):
            raise RuntimeError(f"source partition not found: {source_path}")
        success = self.fs.rename(source_jvm, final_jvm)
        if not success:
            raise RuntimeError(f"rename failed: {source_path} -> {final_path}")

    def _repair_table(self, target_table: str) -> None:
        """Helper interno que realiza la operación "repair_table"."""
        self.spark.sql(f"MSCK REPAIR TABLE {target_table}")

    def _log_staging(self, staging_id: str, execution_id: str, model_id: str, target_table: str, information_date: str, temp_path: str, final_path: str, row_count: int, status: str) -> None:
        """Helper interno que registra staging."""
        now = datetime.now()
        process_date = now.strftime("%Y-%m-%d")
        row = {
            "staging_id": staging_id,
            "execution_id": execution_id,
            "model_id": model_id,
            "target_table": target_table,
            "information_date": information_date,
            "temp_path": temp_path,
            "final_path": final_path,
            "row_count_temp": row_count,
            "row_count_final": None,
            "status": status,
            "started_at": now,
            "validated_at": now,
            "promoted_at": None,
            "process_date": process_date,
        }
        control_df = self.spark.createDataFrame([Row(**row)])
        control_df.write.mode("append").insertInto(self.control_table)

    def _update_staging_status(self, staging_id: str, status: str, row_count_final: int) -> None:
        """Helper interno que actualiza staging status."""
        self.spark.sql(
            f"""
            INSERT OVERWRITE TABLE {self.control_table}
            SELECT
                t.staging_id,
                t.execution_id,
                t.model_id,
                t.target_table,
                t.information_date,
                t.temp_path,
                t.final_path,
                t.row_count_temp,
                CASE WHEN t.staging_id = '{staging_id}' THEN {row_count_final} ELSE t.row_count_final END AS row_count_final,
                CASE WHEN t.staging_id = '{staging_id}' THEN '{status}' ELSE t.status END AS status,
                t.started_at,
                t.validated_at,
                CASE WHEN t.staging_id = '{staging_id}' THEN current_timestamp() ELSE t.promoted_at END AS promoted_at,
                t.process_date
            FROM {self.control_table} AS t
        """
        )
