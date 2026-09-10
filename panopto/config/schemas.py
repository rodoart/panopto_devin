"""Módulo schemas con la(s) clase(s) OutputSchemas."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DataType,
)

_TYPE_MAP: Dict[str, DataType] = {
    "string": StringType(),
    "str": StringType(),
    "double": DoubleType(),
    "float": FloatType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "timestamp": TimestampType(),
}


class OutputSchemas:
    """Carga y expone los esquemas de tablas de salida.

    Busca ``config/output_schemas.json`` relativo a la raíz del repo, salvo que se
    defina ``PANOPTO_OUTPUT_SCHEMAS_PATH``.
    """

    _instance: Optional["OutputSchemas"] = None

    def __new__(cls, path: Optional[str] = None) -> "OutputSchemas":
        if cls._instance is None or path is not None:
            cls._instance = super().__new__(cls)
            cls._instance._load(path)
        return cls._instance

    def _load(self, path: Optional[str]) -> None:
        if path is None:
            path = os.environ.get("PANOPTO_OUTPUT_SCHEMAS_PATH")
        if path is None:
            repo_root = Path(__file__).resolve().parents[2]
            path = repo_root / "config" / "output_schemas.json"
        with open(path, "r", encoding="utf-8") as f:
            self._schemas: Dict[str, List[Dict[str, Any]]] = json.load(f)

    def get(self, table_name: str) -> StructType:
        """Devuelve el ``StructType`` de una tabla de salida."""
        if table_name not in self._schemas:
            raise KeyError(f"schema not found for {table_name}")
        fields = []
        for col in self._schemas[table_name]:
            t = _TYPE_MAP.get(str(col.get("type", "string")).lower())
            if t is None:
                raise ValueError(f"unsupported type {col.get('type')} for {col['name']}")
            fields.append(StructField(col["name"], t, col.get("nullable", True)))
        return StructType(fields)

    def columns(self, table_name: str) -> List[str]:
        """Devuelve la lista de nombres de columnas de una tabla."""
        return [c["name"] for c in self._schemas.get(table_name, [])]

    def normalize_rows(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Asegura que cada fila contenga todas las columnas del esquema, rellenando ``None``."""
        defaults = {c["name"]: None for c in self._schemas.get(table_name, [])}
        out = []
        for row in rows:
            normalized = dict(defaults)
            normalized.update(row)
            out.append(normalized)
        return out
