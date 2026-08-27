"""Módulo reader con la(s) clase(s) DataReader."""

from typing import List, Optional, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from mecv.data.sources import DataSourceSpec
from mecv.logging import get_logger

logger = get_logger(__name__)


class DataReader:
    """Clase que representa DataReader."""
    def __init__(self, spark: SparkSession) -> None:
        """Inicializa una nueva instancia de DataReader."""
        self.spark = spark

    def read(
        self,
        spec: DataSourceSpec,
        reading_dates: Union[str, List[str]],
        extra_cols: Optional[List[str]] = None,
    ) -> DataFrame:
        """Método que lee."""
        if isinstance(reading_dates, str):
            reading_dates = [reading_dates]

        logger.info(f"reading {spec.source_type} {spec.table_or_path} for dates {reading_dates}")
        if spec.source_type == "HIVE":
            full_table = f"{spec.schema}.{spec.table_or_path}" if spec.schema else spec.table_or_path
            df = self.spark.table(full_table)
        elif spec.source_type == "PARQUET":
            df = self.spark.read.parquet(spec.table_or_path)
        else:
            raise ValueError(f"source_type {spec.source_type} not supported")

        if spec.information_date_column and reading_dates:
            df = df.filter(F.col(spec.information_date_column).isin(reading_dates))
        for col in spec.partition_columns:
            if col in df.columns and reading_dates:
                df = df.filter(F.col(col).isin(reading_dates))

        extras = [c for c in (extra_cols or []) if c in df.columns]
        select_cols = [c for c in [spec.column, spec.information_date_column] + extras if c and c in df.columns]
        select_cols = list(dict.fromkeys(select_cols))
        return df.select(*select_cols)
