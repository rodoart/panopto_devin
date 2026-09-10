"""Módulo sources con la(s) clase(s) DataSourceSpec."""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from panopto.config.model_tables import ModelTableConfig


@dataclass
class DataSourceSpec:
    """Clase de datos que representa DataSourceSpec."""
    source_type: str
    schema: Optional[str]
    table_or_path: str
    column: str
    information_date_column: Optional[str]
    partition_columns: List[str] = field(default_factory=list)
    key_columns: List[str] = field(default_factory=list)
    canonical_keys: List[str] = field(default_factory=list)
    date_column: Optional[str] = None
    date_format: Optional[str] = None
    history_months: Optional[int] = None
    lag: Optional[int] = None
    sql_transform: Optional[str] = None
    data_type: Optional[str] = None
    table_role: Optional[str] = None
    table_name: Optional[str] = None

    @classmethod
    def from_metadata(
        cls,
        source_table: str,
        source_column: str,
        information_date_column: str,
        partition_columns: Optional[str] = None,
        table_config: Optional[ModelTableConfig] = None,
    ) -> "DataSourceSpec":
        """Método de clase que realiza la operación "from_metadata"."""
        if table_config is not None:
            return cls.from_model_table(table_config, source_column, information_date_column)

        prefix, _, rest = source_table.partition(":")
        source_type = prefix.upper() if prefix else "HIVE"
        if source_type == "HIVE":
            parts = rest.split(".", 1)
            schema = parts[0] if len(parts) == 2 else None
            table_or_path = parts[-1]
        else:
            schema = None
            table_or_path = rest

        partition_cols = json.loads(partition_columns) if partition_columns else []

        return cls(
            source_type=source_type,
            schema=schema,
            table_or_path=table_or_path,
            column=source_column,
            information_date_column=information_date_column,
            partition_columns=partition_cols,
        )

    @classmethod
    def from_model_table(
        cls,
        table_config: ModelTableConfig,
        source_column: str,
        information_date_column: str,
    ) -> "DataSourceSpec":
        """Construye un DataSourceSpec a partir de ModelTableConfig."""
        if table_config.source_type == "HIVE":
            parts = table_config.source_table.split(".", 1)
            schema = parts[0] if len(parts) == 2 else None
            table_or_path = parts[-1]
        else:
            schema = table_config.source_schema
            table_or_path = table_config.source_table

        return cls(
            source_type=table_config.source_type,
            schema=schema,
            table_or_path=table_or_path,
            column=source_column,
            information_date_column=information_date_column,
            partition_columns=list(table_config.partition_columns),
            key_columns=list(table_config.entity_key_columns),
            canonical_keys=list(table_config.canonical_key_columns),
            date_column=table_config.date_column or information_date_column,
            date_format=table_config.date_format,
            history_months=table_config.history_months,
            lag=table_config.lag,
            sql_transform=table_config.sql_transform,
            data_type=table_config.data_type,
            table_role=table_config.table_role,
            table_name=table_config.table_name,
        )
