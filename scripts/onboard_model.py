"""Script de onboarding: inserta la configuración mínima para un nuevo modelo.

Edite el diccionario MODEL al inicio del archivo con la información del modelo,
las tablas fuente, las llaves y las variables, y luego ejecute:

    python scripts/onboard_model.py

Requiere que las variables de entorno en `.env` apunten al Hive y PostgreSQL correctos.
"""

from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from panopto.config.tables import PROCESS_CONFIG
from panopto.sessions import PostgresSession, SparkSessionBuilder

MODEL: Dict[str, Any] = {
    "model_id": "nuevo_modelo",
    "process_date": datetime.now().strftime("%Y-%m-%d"),
    "model_summary": {
        "model_name": "Nuevo modelo",
        "model_description": "Descripción del modelo",
        "model_type": "binary",
        "status": "active",
        "cut_off_probability": 0.5,
        "frequency": "daily",
        "window_value": 3,
        "window_unit": "weeks",
        "trigger_csi_ambar": 0.1,
        "trigger_csi_red": 0.2,
        "trigger_csi_variation_ambar": 0.05,
        "trigger_csi_variation_red": 0.1,
        "score_alert_ambar_pct": 0.3,
        "score_alert_red_pct": 0.15,
        "score_red_equivalent": 2,
    },
    "tables": [
        {
            "table_role": "raw",
            "table_name": "raw_nuevo_modelo",
            "source_type": "HIVE",
            "source_schema": "gcprmsbx_work",
            "source_table": "hive:gcprmsbx_work.raw_nuevo_modelo",
            "entity_key_columns": ["customer_id"],
            "canonical_key_columns": ["customer_id"],
            "date_column": "information_date",
            "date_format": "",
            "history_months": 3,
            "lag": 0,
            "sql_transform": "",
            "data_type": "",
            "partition_columns": ["information_date"],
            "active": True,
        },
        {
            "table_role": "score",
            "table_name": "score_nuevo_modelo",
            "source_type": "HIVE",
            "source_schema": "gcprmsbx_work",
            "source_table": "hive:gcprmsbx_work.score_nuevo_modelo",
            "entity_key_columns": ["customer_id"],
            "canonical_key_columns": ["customer_id"],
            "date_column": "information_date",
            "date_format": "",
            "history_months": 3,
            "lag": 0,
            "sql_transform": "",
            "data_type": "",
            "partition_columns": ["information_date"],
            "active": True,
        },
        {
            "table_role": "target",
            "table_name": "target_nuevo_modelo",
            "source_type": "HIVE",
            "source_schema": "gcprmsbx_work",
            "source_table": "hive:gcprmsbx_work.target_nuevo_modelo",
            "entity_key_columns": ["customer_id"],
            "canonical_key_columns": ["customer_id"],
            "date_column": "information_date",
            "date_format": "",
            "history_months": 6,
            "lag": 1,
            "sql_transform": "",
            "data_type": "",
            "partition_columns": ["information_date"],
            "active": True,
        },
    ],
    "variables": [
        {
            "variable": "var_1",
            "var_type": "raw",
            "data_type": "numeric",
            "source_table": "hive:gcprmsbx_work.raw_nuevo_modelo",
            "source_column": "var_1",
            "is_monotonic": False,
        },
        {
            "variable": "score",
            "var_type": "score",
            "data_type": "numeric",
            "source_table": "hive:gcprmsbx_work.score_nuevo_modelo",
            "source_column": "score",
            "is_monotonic": False,
        },
        {
            "variable": "target",
            "var_type": "target",
            "data_type": "binary",
            "source_table": "hive:gcprmsbx_work.target_nuevo_modelo",
            "source_column": "target",
            "is_monotonic": False,
        },
    ],
    "contacts": [
        {
            "contact_email": "model-owner@example.com",
            "contact_role": "owner",
            "notify_on_ambar": True,
            "notify_on_red": True,
            "notify_on_missing": True,
        },
    ],
}


def _add_partition(rows: List[Dict[str, Any]], model_id: str, process_date: str) -> List[Dict[str, Any]]:
    return [{**r, "model_id": model_id, "process_date": process_date} for r in rows]


def _to_json_list(value: Any) -> str:
    import json
    if value is None:
        return "[]"
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


def insert_hive(spark: SparkSession) -> None:
    model_id = MODEL["model_id"]
    process_date = MODEL["process_date"]

    # model_summary_csi_psi
    summary_rows = _add_partition([MODEL["model_summary"]], model_id, process_date)
    spark.createDataFrame(summary_rows).write.insertInto(PROCESS_CONFIG.model_summary_table, overwrite=False)
    print(f"inserted {len(summary_rows)} row(s) into {PROCESS_CONFIG.model_summary_table}")

    # panopto_model_table_config
    table_rows = []
    for t in MODEL["tables"]:
        table_rows.append({
            "table_role": t["table_role"],
            "table_name": t["table_name"],
            "source_type": t["source_type"],
            "source_schema": t.get("source_schema", ""),
            "source_table": t["source_table"],
            "entity_key_columns": _to_json_list(t.get("entity_key_columns", [])),
            "canonical_key_columns": _to_json_list(t.get("canonical_key_columns", [])),
            "date_column": t.get("date_column", ""),
            "date_format": t.get("date_format", ""),
            "history_months": t.get("history_months"),
            "lag": t.get("lag"),
            "sql_transform": t.get("sql_transform", ""),
            "data_type": t.get("data_type", ""),
            "partition_columns": _to_json_list(t.get("partition_columns", [])),
            "reading_mode": t.get("reading_mode", "each"),
            "active": t.get("active", True),
        })
    table_rows = _add_partition(table_rows, model_id, process_date)
    spark.createDataFrame(table_rows).write.insertInto(PROCESS_CONFIG.model_table_config_table, overwrite=False)
    print(f"inserted {len(table_rows)} row(s) into {PROCESS_CONFIG.model_table_config_table}")

    # variable_metadata
    var_rows = []
    for v in MODEL["variables"]:
        var_rows.append({
            "variable": v["variable"],
            "var_type": v["var_type"],
            "data_type": v["data_type"],
            "source_table": v["source_table"],
            "source_column": v["source_column"],
            "is_monotonic": v.get("is_monotonic", False),
        })
    var_rows = _add_partition(var_rows, model_id, process_date)
    spark.createDataFrame(var_rows).write.insertInto(PROCESS_CONFIG.variable_metadata_table, overwrite=False)
    print(f"inserted {len(var_rows)} row(s) into {PROCESS_CONFIG.variable_metadata_table}")


def insert_postgres() -> None:
    model_id = MODEL["model_id"]
    process_date = MODEL["process_date"]
    psql = PostgresSession()
    for c in MODEL["contacts"]:
        psql.execute(
            f"""
            INSERT INTO {PROCESS_CONFIG.model_contact_table}
            (model_id, contact_email, contact_role, notify_on_ambar, notify_on_red, notify_on_missing, process_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (model_id, contact_email, process_date) DO NOTHING
            """,
            (
                model_id,
                c["contact_email"],
                c.get("contact_role", ""),
                c.get("notify_on_ambar", True),
                c.get("notify_on_red", True),
                c.get("notify_on_missing", True),
                process_date,
            ),
        )
    print(f"inserted {len(MODEL['contacts'])} contact(s) into {PROCESS_CONFIG.model_contact_table}")


def main() -> None:
    spark = SparkSessionBuilder(app_name="panopto-onboarding").build()
    insert_hive(spark)
    insert_postgres()
    print("Onboarding completado. Ejecute panopto_config_watcher para entrenar el modelo.")


if __name__ == "__main__":
    main()
