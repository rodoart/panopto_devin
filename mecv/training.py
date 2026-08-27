"""Módulo training con la(s) clase(s) TrainingMode."""

from datetime import datetime
from typing import Any, Dict, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from mecv.binning import categorical_bins, compute_bin_counts, numeric_bins
from mecv.data.reader import DataReader
from mecv.data.sources import DataSourceSpec
from mecv.io.atomic_parquet_writer import AtomicParquetWriter
from mecv.logging import get_logger
from mecv.sessions import PostgresSession

logger = get_logger(__name__)


class TrainingMode:
    """Clase que representa TrainingMode."""
    def __init__(self, spark: SparkSession, reader: DataReader) -> None:
        """Inicializa una nueva instancia de TrainingMode."""
        self.spark = spark
        self.reader = reader

    def _load_variable_metadata(self, model_id: str) -> List[Dict[str, Any]]:
        """Helper interno que carga variable metadata."""
        df = self.spark.sql(f"""
            SELECT * FROM variable_metadata_d_t_d
            WHERE process_date = (
                SELECT max(process_date) FROM variable_metadata_d_t_d WHERE model_id = '{model_id}'
            ) AND model_id = '{model_id}'
        """)
        return [r.asDict() for r in df.collect()]

    def _load_category_policy(self, model_id: str) -> Dict[str, Dict[str, Any]]:
        """Helper interno que carga category policy."""
        df = self.spark.sql(f"""
            SELECT * FROM category_policy_d_t_d
            WHERE process_date = (
                SELECT max(process_date) FROM category_policy_d_t_d WHERE model_id = '{model_id}'
            ) AND model_id = '{model_id}'
        """)
        return {r["variable"]: r.asDict() for r in df.collect()}

    def _read_dev_data(self, spec: DataSourceSpec, variable: str) -> DataFrame:
        """Helper interno que lee dev data."""
        if spec.source_type == "HIVE":
            full_table = f"{spec.schema}.{spec.table_or_path}" if spec.schema else spec.table_or_path
            df = self.spark.table(full_table)
        elif spec.source_type == "PARQUET":
            df = self.spark.read.parquet(spec.table_or_path)
        else:
            raise ValueError(f"unsupported source_type: {spec.source_type}")
        if spec.information_date_column and spec.information_date_column in df.columns:
            max_date = df.agg(F.max(spec.information_date_column).alias("m")).collect()[0]["m"]
            df = df.filter(F.col(spec.information_date_column) == max_date)
        df = df.withColumnRenamed(spec.column, variable)
        return df

    @staticmethod
    def _null_rate(df: DataFrame, variable: str) -> float:
        """Helper interno que realiza la operación "null_rate"."""
        total = df.count()
        if total == 0:
            return 0.0
        nulls = df.select(F.sum(F.when(F.col(variable).isNull(), 1).otherwise(0)).alias("n")).collect()[0]["n"]
        return (nulls or 0) / total

    @staticmethod
    def _outlier_rate(df: DataFrame, variable: str) -> float:
        """Helper interno que realiza la operación "outlier_rate"."""
        total = df.count()
        if total == 0:
            return 0.0
        row = df.select(
            F.percentile_approx(F.col(variable), 0.25).alias("q1"),
            F.percentile_approx(F.col(variable), 0.75).alias("q3"),
        ).collect()[0]
        if row is None or row[0] is None or row[1] is None:
            return 0.0
        q1, q3 = row[0], row[1]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = df.filter((F.col(variable) < lower) | (F.col(variable) > upper)).count()
        return outliers / total

    @staticmethod
    def _entropy(df: DataFrame, variable: str) -> float:
        """Helper interno que realiza la operación "entropy"."""
        total = df.count()
        if total == 0:
            return 0.0
        bin_col = F.least(F.floor(F.col(variable) * 10), F.lit(9)).alias("bin")
        return df.withColumn("bin", bin_col).groupBy("bin").count().withColumn(
            "p", F.col("count") / total
        ).agg(F.sum(-F.col("p") * F.log(F.col("p"))).alias("entropy")).collect()[0]["entropy"] or 0.0

    def _auto_thresholds(
        self,
        df: DataFrame,
        variable: str,
        data_type: str,
        var_type: str,
        sample_size: int,
    ) -> List[Dict[str, Any]]:
        """Helper interno que realiza la operación "auto_thresholds"."""
        rows = []
        rows.append({
            "variable": variable,
            "metric_name": "null_rate",
            "threshold_ambar": 0.05,
            "threshold_red": 0.10,
            "baseline_value": self._null_rate(df, variable),
            "baseline_std": 0.0,
            "sample_size_dev": sample_size,
            "calculation_method": "auto",
        })
        if data_type == "numeric":
            rows.append({
                "variable": variable,
                "metric_name": "outlier_rate",
                "threshold_ambar": 0.03,
                "threshold_red": 0.06,
                "baseline_value": self._outlier_rate(df, variable),
                "baseline_std": 0.0,
                "sample_size_dev": sample_size,
                "calculation_method": "auto",
            })
        rows.append({
            "variable": variable,
            "metric_name": "psi_canonical",
            "threshold_ambar": 0.10,
            "threshold_red": 0.20,
            "baseline_value": 0.0,
            "baseline_std": 0.0,
            "sample_size_dev": sample_size,
            "calculation_method": "auto",
        })
        rows.append({
            "variable": variable,
            "metric_name": "psi_dynamic",
            "threshold_ambar": 0.10,
            "threshold_red": 0.20,
            "baseline_value": 0.0,
            "baseline_std": 0.0,
            "sample_size_dev": sample_size,
            "calculation_method": "auto",
        })
        if var_type == "score":
            rows.append({
                "variable": variable,
                "metric_name": "entropy",
                "threshold_ambar": 0.15,
                "threshold_red": 0.30,
                "baseline_value": self._entropy(df, variable),
                "baseline_std": 0.0,
                "sample_size_dev": sample_size,
                "calculation_method": "auto",
            })
        return rows

    def run(self, model_id: str, process_date: str, execution_id: str) -> bool:
        """Método que ejecuta."""
        model_id = str(model_id)
        process_date = str(process_date)
        logger.info(f"starting training for model {model_id}, process_date {process_date}")
        variables = self._load_variable_metadata(model_id)
        category_policy = self._load_category_policy(model_id)
        writer = AtomicParquetWriter(self.spark)
        csi_rows = []
        category_rows = []
        metric_rows = []

        for var in variables:
            variable = var["variable"]
            var_type = var["var_type"]
            data_type = var["data_type"]
            info_col = var["information_date_column"]
            spec = DataSourceSpec.from_metadata(
                source_table=var["source_table"],
                source_column=var["source_column"],
                information_date_column=info_col,
                partition_columns=var["partition_columns"],
            )
            df = self._read_dev_data(spec, variable)
            sample_size = df.count()
            if sample_size == 0:
                continue

            if data_type == "categorical":
                bins = categorical_bins(df, variable, top_n=50)
                total = sample_size
                ranked = df.groupBy(F.col(variable)).count().withColumn(
                    "freq_dev", F.col("count") / total
                ).withColumn(
                    "rank_dev", F.row_number().over(Window.orderBy(F.desc("count")))
                ).collect()
                policy = category_policy.get(variable, {})
                top_n = policy.get("top_n_threshold", 50) if policy else 50
                critical_k = policy.get("critical_top_k", 5) if policy else 5
                for r in ranked:
                    category_rows.append({
                        "variable": variable,
                        "category_value": r[variable],
                        "rank_dev": r["rank_dev"],
                        "freq_dev": r["freq_dev"],
                        "top_n_threshold": top_n,
                        "critical_top_k": critical_k,
                        "process_date": process_date,
                        "model_id": model_id,
                    })
            else:
                bins = numeric_bins(df, variable, n_bins=10)
                bins = compute_bin_counts(df, variable, bins)

            for b in bins:
                b["schema"] = spec.schema or ""
                b["table_name"] = spec.table_or_path
                b["type"] = var_type
                b["variable"] = variable
                b["information_date_column"] = info_col
                b["process_date"] = process_date
                b["model_id"] = model_id
                if "woe" not in b:
                    b["woe"] = 0.0
                csi_rows.append(b)

            metric_rows.extend(
                self._auto_thresholds(df, variable, data_type, var_type, sample_size)
            )

        if csi_rows:
            csi_df = self.spark.createDataFrame(csi_rows)
            writer.write_atomic(csi_df, "csi_psi_table_d_t_d", model_id, process_date, execution_id, partition_cols=["process_date", "model_id"])
        if category_rows:
            cat_df = self.spark.createDataFrame(category_rows)
            writer.write_atomic(cat_df, "category_baseline_rank_d_t_d", model_id, process_date, execution_id, partition_cols=["process_date", "model_id"])
        if metric_rows:
            mt_df = self.spark.createDataFrame(metric_rows)
            writer.write_atomic(mt_df, "metric_threshold_auto_d_t_d", model_id, process_date, execution_id, partition_cols=["process_date", "model_id"])
        return True
