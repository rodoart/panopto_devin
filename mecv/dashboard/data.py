"""Módulo de acceso a datos para dashboards MECV."""

from typing import Any, Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from mecv.sessions import SparkSessionBuilder


class DashboardData:
    """Conexión a las vistas de dashboard en Hive/Spark."""

    def __init__(self) -> None:
        """Inicializa una sesión Spark para leer las vistas de dashboard."""
        self.spark: SparkSession = SparkSessionBuilder(app_name="mecv-dashboard").build()

    def get_models(self) -> list:
        """Lista de model_id disponibles."""
        df = self.spark.sql(
            "SELECT DISTINCT model_id FROM mecv_dashboard_model_summary ORDER BY model_id"
        )
        return [r["model_id"] for r in df.collect()]

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
        """Carga mecv_dashboard_semaphore con filtros."""
        df = self._apply_filters(
            self.spark.table("mecv_dashboard_semaphore"), model_id, start, end
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
        """Carga mecv_dashboard_model_summary con filtros."""
        df = self._apply_filters(
            self.spark.table("mecv_dashboard_model_summary"), model_id, start, end
        ).orderBy("information_date")
        pdf = df.toPandas()
        if not pdf.empty:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf
