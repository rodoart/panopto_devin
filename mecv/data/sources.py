import json
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DataSourceSpec:
    source_type: str
    schema: Optional[str]
    table_or_path: str
    column: str
    information_date_column: str
    partition_columns: List[str]

    @classmethod
    def from_metadata(cls, source_table: str, source_column: str, information_date_column: str, partition_columns: str = "[]"):
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
