"""Módulo de acceso a datos para dashboards PANOPTO."""

from typing import Any, Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from panopto.config.tables import PROCESS_CONFIG
from panopto.sessions import SparkSessionBuilder


class DashboardData:
    """Conexión a las vistas de dashboard en Hive/Spark."""

    def __init__(self) -> None:
        """Inicializa una sesión Spark para leer las vistas de dashboard."""
        self.spark: SparkSession = SparkSessionBuilder(app_name="panopto-dashboard").build()

    def get_models(self) -> list:
        """Lista de model_id disponibles."""
        df = self.spark.sql(
            f"SELECT DISTINCT model_id FROM {PROCESS_CONFIG.dashboard_model_summary_table} ORDER BY model_id"
        )
        return [r["model_id"] for r in df.collect()]

    def get_date_range(self, model_id: Optional[str] = None) -> tuple:
        """Devuelve (min_date, max_date) de information_date para el modelo dado."""
        df = self.spark.table(PROCESS_CONFIG.dashboard_model_summary_table)
        if model_id:
            df = df.filter(F.col("model_id") == model_id)
        row = df.agg(
            F.min("information_date").alias("min_d"),
            F.max("information_date").alias("max_d"),
        ).collect()[0]
        return row["min_d"], row["max_d"]

    @staticmethod
    def _apply_filters(df: Any, model_id: Optional[str], start: Optional[str], end: Optional[str]) -> Any:
        """Aplica filtros seguros de modelo y rango de fechas."""
        if model_id:
            df = df.filter(F.col("model_id") == model_id)
        if start:
            df = df.filter(F.col("information_date") >= start)
        if end:
            df = df.filter(F.col("information_date") <= end)
        return df

    def get_semaphore(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga panopto_dashboard_semaphore con filtros."""
        df = self._apply_filters(
            self.spark.table(PROCESS_CONFIG.dashboard_semaphore_table), model_id, start, end
        ).orderBy("information_date")
        pdf = df.toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf

    def get_summary(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga panopto_dashboard_model_summary con filtros."""
        df = self._apply_filters(
            self.spark.table(PROCESS_CONFIG.dashboard_model_summary_table), model_id, start, end
        ).orderBy("information_date")
        pdf = df.toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf

    def get_scoring_summary(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga la fila canónica "Summary Scoring Monitoring" (panopto_scoring_summary)."""
        df = self._apply_filters(
            self.spark.table(PROCESS_CONFIG.scoring_summary_table), model_id, start, end
        ).orderBy("model_id", "information_date")
        pdf = df.toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf

    def get_data_availability(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga el control "Data Availability Monitoring" (panopto_data_availability)."""
        df = self._apply_filters(
            self.spark.table(PROCESS_CONFIG.data_availability_table), model_id, start, end
        ).orderBy("model_id", "information_date", "source_table")
        pdf = df.toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf

    def get_metric_results(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        var_types: Optional[list] = None,
        metric_names: Optional[list] = None,
    ) -> pd.DataFrame:
        """Carga panopto_metric_result con filtros por var_type y metric_name."""
        df = self._apply_filters(
            self.spark.table(PROCESS_CONFIG.metric_result_table), model_id, start, end
        )
        if var_types:
            df = df.filter(F.col("var_type").isin(var_types))
        if metric_names:
            df = df.filter(F.col("metric_name").isin(metric_names))
        pdf = df.orderBy("information_date").toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf

    def get_variable_summary(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        var_types: Optional[list] = None,
        statistics: Optional[list] = None,
    ) -> pd.DataFrame:
        """Carga panopto_variable_summary con filtros por var_type y statistic."""
        df = self._apply_filters(
            self.spark.table(PROCESS_CONFIG.variable_summary_table), model_id, start, end
        )
        if var_types:
            df = df.filter(F.col("var_type").isin(var_types))
        if statistics:
            df = df.filter(F.col("statistic").isin(statistics))
        pdf = df.orderBy("information_date").toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf
