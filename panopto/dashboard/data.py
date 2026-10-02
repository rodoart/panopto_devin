"""Módulo de acceso a datos para dashboards PANOPTO.

El dashboard lee exclusivamente de las copias en PostgreSQL de las tablas/vistas
Hive (``panopto_*`` sin esquema); nada se consulta directamente a Spark. Esas
copias las mantiene el DAG ``panopto_dashboard_sync``
(``panopto/dashboard/pg_sync.py``).
"""

from typing import Any, List, Optional, Tuple

import pandas as pd

from panopto.config.tables import PROCESS_CONFIG
from panopto.sessions import PostgresSession


def _iso(value: Any) -> str:
    """Normaliza una fecha (str, date o Timestamp) a ISO YYYY-MM-DD."""
    return pd.Timestamp(value).date().isoformat()


class DashboardData:
    """Conexión a las copias PostgreSQL de las tablas del dashboard."""

    def __init__(self) -> None:
        """Inicializa el acceso a PostgreSQL (sin Spark)."""
        self.psql = PostgresSession()

    def _query(self, sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
        with self.psql.connection() as conn:
            pdf = pd.read_sql(sql, conn, params=params or [])
        if not pdf.empty and "information_date" in pdf.columns:
            pdf["information_date"] = pd.to_datetime(pdf["information_date"])
        return pdf

    @staticmethod
    def _where(
        model_id: Optional[str],
        start: Optional[Any],
        end: Optional[Any],
        in_filters: Optional[List[Tuple[str, Optional[list]]]] = None,
    ) -> Tuple[str, List[Any]]:
        """Construye el WHERE parametrizado (modelo, rango de fechas y filtros IN)."""
        clauses, params = [], []
        if model_id:
            clauses.append("model_id = %s")
            params.append(model_id)
        if start:
            clauses.append("information_date >= %s")
            params.append(_iso(start))
        if end:
            clauses.append("information_date <= %s")
            params.append(_iso(end))
        for column, values in in_filters or []:
            if values:
                clauses.append(f"{column} = ANY(%s)")
                params.append(list(values))
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", params

    def get_models(self) -> list:
        """Lista de model_id disponibles."""
        pdf = self._query(
            f"SELECT DISTINCT model_id FROM {PROCESS_CONFIG.pg_dashboard_model_summary_table} ORDER BY model_id"
        )
        return pdf["model_id"].tolist()

    def get_date_range(self, model_id: Optional[str] = None) -> tuple:
        """Devuelve (min_date, max_date) de information_date para el modelo dado."""
        where, params = self._where(model_id, None, None)
        pdf = self._query(
            f"SELECT MIN(information_date) AS min_d, MAX(information_date) AS max_d "
            f"FROM {PROCESS_CONFIG.pg_dashboard_model_summary_table}{where}",
            params,
        )
        row = pdf.iloc[0]
        return row["min_d"], row["max_d"]

    def get_semaphore(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga panopto_dashboard_semaphore con filtros."""
        where, params = self._where(model_id, start, end)
        return self._query(
            f"SELECT * FROM {PROCESS_CONFIG.pg_dashboard_semaphore_table}{where} ORDER BY information_date",
            params,
        )

    def get_summary(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga panopto_dashboard_model_summary con filtros."""
        where, params = self._where(model_id, start, end)
        return self._query(
            f"SELECT * FROM {PROCESS_CONFIG.pg_dashboard_model_summary_table}{where} ORDER BY information_date",
            params,
        )

    def get_scoring_summary(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga la fila canónica "Summary Scoring Monitoring" (panopto_scoring_summary)."""
        where, params = self._where(model_id, start, end)
        return self._query(
            f"SELECT * FROM {PROCESS_CONFIG.pg_scoring_summary_table}{where} "
            f"ORDER BY model_id, information_date",
            params,
        )

    def get_data_availability(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Carga el control "Data Availability Monitoring" (panopto_data_availability)."""
        where, params = self._where(model_id, start, end)
        return self._query(
            f"SELECT * FROM {PROCESS_CONFIG.pg_data_availability_table}{where} "
            f"ORDER BY model_id, information_date, source_table",
            params,
        )

    def get_metric_results(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        var_types: Optional[list] = None,
        metric_names: Optional[list] = None,
    ) -> pd.DataFrame:
        """Carga panopto_metric_result con filtros por var_type y metric_name."""
        where, params = self._where(
            model_id, start, end, [("var_type", var_types), ("metric_name", metric_names)]
        )
        return self._query(
            f"SELECT * FROM {PROCESS_CONFIG.pg_metric_result_table}{where} ORDER BY information_date",
            params,
        )

    def get_variable_summary(
        self,
        model_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        var_types: Optional[list] = None,
        statistics: Optional[list] = None,
    ) -> pd.DataFrame:
        """Carga panopto_variable_summary con filtros por var_type y statistic."""
        where, params = self._where(
            model_id, start, end, [("var_type", var_types), ("statistic", statistics)]
        )
        return self._query(
            f"SELECT * FROM {PROCESS_CONFIG.pg_variable_summary_table}{where} ORDER BY information_date",
            params,
        )
