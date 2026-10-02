"""Sincroniza las tablas del dashboard de Hive/Spark hacia PostgreSQL.

El dashboard Streamlit (``panopto/dashboard/app.py``) lee exclusivamente de las
copias en PostgreSQL; este módulo las refresca de forma incremental:

- Por cada tabla se comparan las particiones ``(information_date, model_id)``
  de Hive contra Postgres.
- Las particiones presentes en Hive se reemplazan siempre (``DELETE`` +
  ``INSERT``): un reproceso puede cambiar valores sin cambiar el conteo, así
  que comparar por número de filas dejaría datos obsoletos. Las que solo
  existen en Postgres se borran.
- Se puede acotar a un ``information_date``/``model_id`` (p. ej. el ``conf`` del
  DAG que dispara la corrida); el diff se aplica solo dentro de ese ámbito.
- Las tablas Postgres se crean si no existen: las de tablas físicas derivan sus
  columnas de ``config/output_schemas.json`` y las de las vistas Hive usan una
  lista fija de columnas.
"""

from typing import Any, Dict, List, Optional, Tuple

from panopto.config.schemas import OutputSchemas
from panopto.config.tables import PROCESS_CONFIG
from panopto.logging import get_logger

logger = get_logger(__name__)

# Tablas/vistas de Hive que alimentan el dashboard -> espejo en PostgreSQL.
DASHBOARD_TABLES: List[Tuple[str, str]] = [
    (PROCESS_CONFIG.metric_result_table, PROCESS_CONFIG.pg_metric_result_table),
    (PROCESS_CONFIG.variable_summary_table, PROCESS_CONFIG.pg_variable_summary_table),
    (PROCESS_CONFIG.scoring_summary_table, PROCESS_CONFIG.pg_scoring_summary_table),
    (PROCESS_CONFIG.data_availability_table, PROCESS_CONFIG.pg_data_availability_table),
    (PROCESS_CONFIG.dashboard_semaphore_table, PROCESS_CONFIG.pg_dashboard_semaphore_table),
    (PROCESS_CONFIG.dashboard_model_summary_table, PROCESS_CONFIG.pg_dashboard_model_summary_table),
]

# Columnas de las vistas Hive (no tienen entrada en output_schemas.json por ser
# vistas). Se mantienen alineadas con panopto/dashboard/builder.py.
_VIEW_COLUMNS: Dict[str, List[Tuple[str, str]]] = {
    "panopto_dashboard_semaphore": [
        ("information_date", "text"),
        ("model_id", "text"),
        ("var_type", "text"),
        ("metric_name", "text"),
        ("metric_value", "double precision"),
        ("status", "text"),
        ("aggregate_status", "text"),
        ("stress_ratio", "double precision"),
        ("execution_status", "text"),
        ("variables_missing", "integer"),
    ],
    "panopto_dashboard_model_summary": [
        ("information_date", "text"),
        ("model_id", "text"),
        ("score_status", "text"),
        ("input_status", "text"),
        ("raw_status", "text"),
        ("transformed_status", "text"),
        ("system_status", "text"),
        ("has_missing_data", "integer"),
        ("var_types_evaluated", "integer"),
    ],
}

_SPARK_TO_PG_TYPES = {
    "string": "text",
    "double": "double precision",
    "int": "integer",
    "boolean": "boolean",
    "timestamp": "timestamp",
}

_BATCH_SIZE = 1000


def _pg_columns(schemas: OutputSchemas, hive_table: str, pg_table: str) -> List[Tuple[str, str]]:
    """Columnas (nombre, tipo PG) del espejo, de output_schemas o de la vista."""
    if pg_table in _VIEW_COLUMNS:
        return _VIEW_COLUMNS[pg_table]
    schema = schemas.get(hive_table)
    return [
        (field.name, _SPARK_TO_PG_TYPES.get(field.dataType.simpleString(), "text"))
        for field in schema.fields
    ]


def _ensure_table(cur: Any, columns: List[Tuple[str, str]], pg_table: str) -> None:
    cols = ", ".join(f'"{name}" {ptype}' for name, ptype in columns)
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{pg_table}" ({cols})')


def _hive_partition_counts(
    spark: Any,
    hive_table: str,
    information_date: Optional[str],
    model_id: Optional[str],
) -> Dict[Tuple[str, str], int]:
    where = []
    if information_date:
        where.append(f"information_date = '{information_date}'")
    if model_id:
        where.append(f"model_id = '{model_id}'")
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    rows = spark.sql(f"""
        SELECT information_date, model_id, COUNT(*) AS c
        FROM {hive_table}{where_sql}
        GROUP BY information_date, model_id
    """).collect()
    return {(r["information_date"], r["model_id"]): r["c"] for r in rows}


def _pg_partition_counts(
    cur: Any,
    pg_table: str,
    information_date: Optional[str],
    model_id: Optional[str],
) -> Dict[Tuple[str, str], int]:
    where, params = [], []
    if information_date:
        where.append("information_date = %s")
        params.append(information_date)
    if model_id:
        where.append("model_id = %s")
        params.append(model_id)
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    cur.execute(
        f'SELECT information_date, model_id, COUNT(*) FROM "{pg_table}"{where_sql} GROUP BY 1, 2',
        tuple(params),
    )
    return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def _sync_partition(
    spark: Any,
    cur: Any,
    hive_table: str,
    pg_table: str,
    information_date: Optional[str],
    model_id: Optional[str],
) -> int:
    """Reemplaza una partición en Postgres con las filas de Hive."""
    cur.execute(
        f'DELETE FROM "{pg_table}" WHERE information_date = %s AND model_id = %s',
        (information_date, model_id),
    )
    df = spark.sql(f"""
        SELECT * FROM {hive_table}
        WHERE information_date = '{information_date}' AND model_id = '{model_id}'
    """)
    cols = df.columns
    insert_sql = f'INSERT INTO "{pg_table}" ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})'
    count = 0
    batch: List[tuple] = []
    for row in df.toLocalIterator():
        batch.append(tuple(row))
        if len(batch) >= _BATCH_SIZE:
            cur.executemany(insert_sql, batch)
            count += len(batch)
            batch = []
    if batch:
        cur.executemany(insert_sql, batch)
        count += len(batch)
    return count


def sync_dashboard(
    spark: Any,
    psql: Any,
    information_date: Optional[str] = None,
    model_id: Optional[str] = None,
) -> None:
    """Sincroniza las tablas del dashboard de Hive hacia PostgreSQL.

    Reescribe todas las particiones ``(information_date, model_id)`` de Hive
    dentro del ámbito indicado (un reproceso puede cambiar valores sin cambiar
    el conteo de filas) y elimina particiones huérfanas en Postgres.
    """
    schemas = OutputSchemas()
    scope = f"information_date={information_date or '*'}, model_id={model_id or '*'}"
    logger.info(f"syncing dashboard tables to postgres ({scope})")
    for hive_table, pg_table in DASHBOARD_TABLES:
        try:
            hive_partitions = _hive_partition_counts(spark, hive_table, information_date, model_id)
        except Exception as exc:
            logger.warning(f"cannot read {hive_table}; skipping ({exc})")
            continue
        columns = _pg_columns(schemas, hive_table, pg_table)
        with psql.connection() as conn:
            with conn.cursor() as cur:
                _ensure_table(cur, columns, pg_table)
                pg_partitions = _pg_partition_counts(cur, pg_table, information_date, model_id)
                stale = set(hive_partitions)
                orphan = set(pg_partitions) - set(hive_partitions)
                for info_date, m_id in stale:
                    n = _sync_partition(spark, cur, hive_table, pg_table, info_date, m_id)
                    logger.info(f"{pg_table}: synced partition {info_date}/{m_id} ({n} rows)")
                for info_date, m_id in orphan:
                    cur.execute(
                        f'DELETE FROM "{pg_table}" WHERE information_date = %s AND model_id = %s',
                        (info_date, m_id),
                    )
                    logger.info(f"{pg_table}: dropped orphan partition {info_date}/{m_id}")
            conn.commit()
    logger.info("dashboard sync to postgres finished")
