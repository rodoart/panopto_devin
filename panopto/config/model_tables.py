"""Módulo model_tables con la(s) clase(s) ModelTableConfig."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F

from panopto.config.tables import PROCESS_CONFIG
from panopto.logging import get_logger

logger = get_logger(__name__)


def _parse_json_list(value: Any) -> List[str]:
    """Convierte un JSON array o una lista separada por comas a list[str]."""
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    try:
        parsed = json.loads(value)
        return [str(v) for v in parsed] if isinstance(parsed, list) else []
    except Exception:
        return [str(x).strip() for x in str(value).split(",") if x.strip()]


def _parse_int(value: Any) -> Optional[int]:
    """Convierte un valor a int o devuelve None."""
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


@dataclass
class ModelTableConfig:
    """Configuración a nivel tabla para un modelo."""

    table_role: str
    table_name: str
    source_type: str
    source_schema: Optional[str]
    source_table: str
    entity_key_columns: List[str]
    canonical_key_columns: List[str]
    date_column: Optional[str]
    date_format: Optional[str]
    history_months: Optional[int]
    lag: Optional[int]
    sql_transform: Optional[str]
    data_type: Optional[str]
    partition_columns: List[str]
    reading_mode: Optional[str]
    active: bool = True
    model_id: Optional[str] = None
    process_date: Optional[str] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "ModelTableConfig":
        """Construye una instancia a partir de un diccionario de Hive/Spark."""
        return cls(
            table_role=str(row.get("table_role", "")).lower(),
            table_name=str(row.get("table_name", "")).lower(),
            source_type=str(row.get("source_type", "HIVE")).upper(),
            source_schema=row.get("source_schema") or None,
            source_table=str(row.get("source_table", "")),
            entity_key_columns=_parse_json_list(row.get("entity_key_columns", "[]")),
            canonical_key_columns=_parse_json_list(row.get("canonical_key_columns", "[]")),
            date_column=row.get("date_column") or None,
            date_format=row.get("date_format") or None,
            history_months=_parse_int(row.get("history_months")),
            lag=_parse_int(row.get("lag")),
            sql_transform=row.get("sql_transform") or None,
            data_type=row.get("data_type") or None,
            partition_columns=_parse_json_list(row.get("partition_columns", "[]")),
            reading_mode=row.get("reading_mode") or "each",
            active=bool(row.get("active", True)),
            model_id=row.get("model_id"),
            process_date=row.get("process_date"),
        )

    def format_date(self, iso_date: str) -> str:
        """Convierte una fecha ISO a la representación propia de la tabla."""
        if not self.date_format:
            return iso_date
        try:
            dt = datetime.fromisoformat(iso_date)
            return dt.strftime(self.date_format)
        except Exception:
            logger.warning(f"could not format date {iso_date} with {self.date_format}")
            return iso_date

    def format_dates(self, iso_dates: List[str]) -> List[str]:
        """Formatea una lista de fechas ISO para comparar contra la tabla."""
        return list(dict.fromkeys(self.format_date(d) for d in iso_dates))

    def history_date_range(self, reference_date: str) -> List[str]:
        """Genera un rango de fechas a leer según history_months y lag."""
        dt = datetime.fromisoformat(reference_date)
        lag = self.lag or 0
        history = self.history_months or 1

        end = self._shift_months(dt, -lag)
        start = self._shift_months(end, -(history - 1))

        dates = []
        while start <= end:
            dates.append(start.isoformat()[:10])
            start = self._shift_months(start, 1)
        return dates

    @staticmethod
    def _shift_months(d: datetime, months: int) -> datetime:
        """Desplaza una fecha en meses respetando días válidos."""
        month = d.month - 1 + months
        year = d.year + month // 12
        month = month % 12 + 1
        day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
        return d.replace(year=year, month=month, day=day)


def load_model_table_config(
    spark: Any,
    model_id: str,
    table: Optional[str] = None,
) -> List[ModelTableConfig]:
    """Carga la última partición activa de configuración de tablas por modelo."""
    table = table or PROCESS_CONFIG.model_table_config_table
    table_df = spark.table(table)
    max_row = table_df.filter(F.col("model_id") == model_id).agg(
        F.max("process_date").alias("m")
    ).collect()
    if not max_row or max_row[0]["m"] is None:
        return []
    max_pd = max_row[0]["m"]
    df = table_df.filter(
        (F.col("model_id") == model_id) & (F.col("process_date") == max_pd)
    )
    rows = [r.asDict() for r in df.collect()]
    return [ModelTableConfig.from_row(r) for r in rows if r.get("active") is not False]


def load_model_table_config_map(
    spark: Any,
    model_id: str,
    table: Optional[str] = None,
) -> Dict[str, ModelTableConfig]:
    """Devuelve un mapa {source_table: ModelTableConfig} para un modelo."""
    out = {}
    for cfg in load_model_table_config(spark, model_id, table):
        if not cfg.active:
            continue
        out[cfg.source_table] = cfg
    return out
