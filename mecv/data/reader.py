"""Módulo reader con la(s) clase(s) DataReader."""

import re
from datetime import datetime
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

    @staticmethod
    def _split_expressions(expr: str) -> List[str]:
        """Divide expresiones SQL separadas por comas respetando paréntesis."""
        return [s.strip() for s in re.split(r",\s*(?![^()]*\))", expr) if s.strip()]

    def _load_source(self, spec: DataSourceSpec) -> DataFrame:
        """Carga la fuente cruda (Hive o Parquet)."""
        if spec.source_type == "HIVE":
            full_table = f"{spec.schema}.{spec.table_or_path}" if spec.schema else spec.table_or_path
            df = self.spark.table(full_table)
        elif spec.source_type == "PARQUET":
            df = self.spark.read.parquet(spec.table_or_path)
        else:
            raise ValueError(f"source_type {spec.source_type} not supported")
        return df

    def _apply_transform(self, df: DataFrame, spec: DataSourceSpec) -> DataFrame:
        """Aplica el sql_transform de la tabla, si existe."""
        if not spec.sql_transform:
            return df
        exprs = self._split_expressions(spec.sql_transform)
        return df.selectExpr(*exprs)

    def _format_dates(self, spec: DataSourceSpec, reading_dates: List[str]) -> List[str]:
        """Convierte fechas ISO al formato propio de la tabla."""
        if not spec.date_format:
            return reading_dates
        out = []
        for d in reading_dates:
            try:
                dt = datetime.fromisoformat(d)
                out.append(dt.strftime(spec.date_format))
            except Exception:
                out.append(d)
        return list(dict.fromkeys(out))

    def read(
        self,
        spec: DataSourceSpec,
        reading_dates: Union[str, List[str], None] = None,
        extra_cols: Optional[List[str]] = None,
    ) -> DataFrame:
        """Método que lee."""
        if reading_dates is None:
            reading_dates = []
        elif isinstance(reading_dates, str):
            reading_dates = [reading_dates]
        else:
            reading_dates = list(reading_dates)

        logger.info(f"reading {spec.source_type} {spec.table_or_path} for dates {reading_dates}")
        df = self._load_source(spec)
        df = self._apply_transform(df, spec)

        date_col = spec.date_column or spec.information_date_column
        formatted_dates = self._format_dates(spec, reading_dates)

        select_cols = [c for c in [spec.column, date_col] if c]
        select_cols.extend(c for c in spec.key_columns if c)
        if extra_cols:
            select_cols.extend(c for c in extra_cols if c)

        available = set(df.columns)
        missing = [c for c in select_cols if c not in available]
        if missing:
            raise ValueError(
                f"missing columns {missing} in {spec.table_or_path}; available: {sorted(available)}"
            )

        select_cols = list(dict.fromkeys(select_cols))
        df = df.select(*select_cols)

        if date_col and formatted_dates:
            df = df.filter(F.col(date_col).isin(formatted_dates))

        if spec.canonical_keys and spec.key_columns and len(spec.canonical_keys) == len(spec.key_columns):
            for src, dst in zip(spec.key_columns, spec.canonical_keys):
                if src in df.columns and dst != src:
                    df = df.withColumnRenamed(src, dst)

        return df
