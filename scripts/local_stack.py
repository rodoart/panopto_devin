"""Helpers compartidos del stack local PANOPTO (Spark local + Postgres portable).

Importar siempre DESPUÉS de ``local_env.apply()`` para que ``PROCESS_CONFIG``
se cargue desde ``config/tables.local.json``.
"""

import os
from typing import Any, Dict, List

_SPARK_TO_SQL = {
    "string": "STRING",
    "str": "STRING",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "int": "INT",
    "integer": "INT",
    "long": "BIGINT",
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}

# Tablas panopto_* que en Hive son VISTAS (las crea dashboard/builder.py),
# no tablas físicas.
VIEW_TABLES = {"panopto_dashboard_semaphore", "panopto_dashboard_model_summary"}

# Tablas panopto_* sin entrada en output_schemas.json (fuentes externas).
NO_SCHEMA_TABLES = {"panopto_banamex_calendar_ext_d"}


def spark_conf() -> Dict[str, str]:
    """Conf Spark para el modo local (metastore Hive Derby embebido)."""
    import local_env

    hadoop_home = (local_env.ROOT / ".local" / "hadoop").as_posix()
    java_opts = (
        f"-Dderby.system.home={local_env.ROOT.as_posix()}/.local"
        f" -Djava.library.path={hadoop_home}/bin"
        f" -Dhadoop.home.dir={hadoop_home}"
    )
    return {
        "spark.master": os.environ.get("PANOPTO_LOCAL_MASTER", "local[*]"),
        "spark.sql.catalogImplementation": "hive",
        # El metastore Derby es local: spark.sql.warehouse.dir y
        # PANOPTO_HIVE_WAREHOUSE_DIR (usado por AtomicParquetWriter) deben
        # apuntar a la MISMA ruta o las particiones promovidas no se registran.
        "spark.sql.warehouse.dir": os.environ.get(
            "PANOPTO_HIVE_WAREHOUSE_DIR",
            f"file:///{local_env.ROOT.as_posix()}/spark-warehouse",
        ),
        "spark.sql.shuffle.partitions": "4",
        "spark.driver.host": "localhost",
        "spark.local.dir": (local_env.ROOT / ".local" / "spark_tmp").as_posix(),
        "spark.driver.extraJavaOptions": java_opts,
        "spark.executor.extraJavaOptions": java_opts,
        "spark.sql.legacy.allowNonEmptyLocationInCTAS": "true",
    }


def panopto_table_names(process_config: Any) -> Dict[str, str]:
    """Mapa attr -> nombre de tabla local panopto_* que existe en output_schemas."""
    from panopto.config.schemas import OutputSchemas

    schemas = OutputSchemas()
    out = {}
    for field_name in process_config.__dataclass_fields__:
        value = getattr(process_config, field_name)
        if not isinstance(value, str) or not value.startswith("panopto_"):
            continue
        if value in VIEW_TABLES or value in NO_SCHEMA_TABLES:
            continue
        short = value.removeprefix("panopto_")
        if short in schemas._schemas:
            out[field_name] = value
    return out


def create_panopto_tables(spark: Any) -> List[str]:
    """Crea (si no existen) las tablas panopto_* vacías en el metastore local.

    Las columnas salen de ``config/output_schemas.json``: los campos con
    ``partition: true`` van a ``PARTITIONED BY`` y el resto son columnas
    regulares. Es equivalente a ``sql/ddl_hive.sql`` del cluster.
    """
    from panopto.config.schemas import OutputSchemas
    from panopto.config.tables import PROCESS_CONFIG

    schemas = OutputSchemas()
    created = []
    for table in panopto_table_names(PROCESS_CONFIG).values():
        struct = schemas.get(table)
        sdef = schemas._schemas[schemas._short_name(table)]
        part_names = {f["name"] for f in sdef if f.get("partition")}
        cols, parts = [], []
        for f in struct.fields:
            sql_type = _SPARK_TO_SQL.get(f.dataType.simpleString(), "STRING")
            if f.name in part_names:
                parts.append(f'`{f.name}` {sql_type}')
            else:
                cols.append(f'`{f.name}` {sql_type}')
        if not cols:
            continue
        ddl = f'CREATE TABLE IF NOT EXISTS {table} ({", ".join(cols)}) USING parquet'
        if parts:
            ddl += f' PARTITIONED BY ({", ".join(parts)})'
        spark.sql(ddl)
        created.append(table)
    return created


def insert_rows(spark: Any, table: str, rows: List[Dict[str, Any]]) -> int:
    """Inserta filas (dicts) en una tabla panopto_* respetando su esquema."""
    if not rows:
        return 0
    from panopto.config.schemas import OutputSchemas

    schemas = OutputSchemas()
    schema = schemas.get(table)
    normalized = schemas.normalize_rows(table, rows)
    df = spark.createDataFrame(normalized, schema=schema)
    df.write.mode("append").insertInto(table)
    return len(rows)


def insert_config_rows(
    spark: Any,
    model_id: str,
    process_date: str,
    model_row: Dict[str, Any],
    table_rows: List[Dict[str, Any]],
    var_rows: List[Dict[str, Any]],
    policy_rows: List[Dict[str, Any]],
    threshold_rows: List[Dict[str, Any]],
) -> None:
    """Escribe la configuración del modelo en las tablas panopto_* de config."""
    from panopto.config.tables import PROCESS_CONFIG

    def part(r):
        return {**r, "model_id": model_id, "process_date": process_date}

    insert_rows(spark, PROCESS_CONFIG.model_summary_table, [part(model_row)])
    insert_rows(spark, PROCESS_CONFIG.model_table_config_table, [part(r) for r in table_rows])
    insert_rows(spark, PROCESS_CONFIG.variable_metadata_table, [part(r) for r in var_rows])
    if policy_rows:
        insert_rows(spark, PROCESS_CONFIG.category_policy_table, [part(r) for r in policy_rows])
    if threshold_rows:
        insert_rows(spark, PROCESS_CONFIG.thresholds_table, [part(r) for r in threshold_rows])
