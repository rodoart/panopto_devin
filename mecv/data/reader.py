from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from mecv.data.sources import DataSourceSpec


class DataReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(
        self,
        spec: DataSourceSpec,
        information_date: str,
        extra_cols: Optional[List[str]] = None,
    ) -> DataFrame:
        if spec.source_type == "HIVE":
            full_table = f"{spec.schema}.{spec.table_or_path}" if spec.schema else spec.table_or_path
            df = self.spark.table(full_table)
        elif spec.source_type == "PARQUET":
            df = self.spark.read.parquet(spec.table_or_path)
        else:
            raise ValueError(f"source_type {spec.source_type} not supported")
        if spec.information_date_column and information_date:
            df = df.filter(df[spec.information_date_column] == information_date)
        for col in spec.partition_columns:
            if col in df.columns and information_date:
                df = df.filter(df[col] == information_date)
        extras = [c for c in (extra_cols or []) if c in df.columns]
        select_cols = [c for c in [spec.column, spec.information_date_column] + extras if c and c in df.columns]
        select_cols = list(dict.fromkeys(select_cols))
        return df.select(*select_cols)
