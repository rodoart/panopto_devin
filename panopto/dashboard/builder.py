"""Módulo para construir/refrescar las vistas del dashboard PANOPTO."""

from typing import Any

from panopto.config.tables import PROCESS_CONFIG
from panopto.logging import get_logger

logger = get_logger(__name__)


def build_dashboard_views(spark: Any) -> None:
    """Crea o reemplaza las vistas Hive usadas por el dashboard."""
    metric_result = PROCESS_CONFIG.metric_result_table
    alert_aggregate = PROCESS_CONFIG.alert_aggregate_table
    execution_log = PROCESS_CONFIG.execution_log_table
    dashboard_semaphore = PROCESS_CONFIG.dashboard_semaphore_table
    dashboard_model_summary = PROCESS_CONFIG.dashboard_model_summary_table

    semaphore_sql = f"""
    CREATE OR REPLACE VIEW {dashboard_semaphore} AS
    SELECT
        m.information_date,
        m.model_id,
        m.var_type,
        m.metric_name,
        m.metric_value,
        m.status,
        a.aggregate_status,
        a.stress_ratio,
        e.status AS execution_status,
        e.variables_missing
    FROM {metric_result} m
    LEFT JOIN {alert_aggregate} a
        ON m.model_id = a.model_id
        AND m.information_date = a.information_date
    LEFT JOIN {execution_log} e
        ON m.model_id = e.model_id
        AND m.information_date = e.information_date
    """

    summary_sql = f"""
    CREATE OR REPLACE VIEW {dashboard_model_summary} AS
    SELECT
        information_date,
        model_id,
        MAX(CASE WHEN var_type = 'score' THEN aggregate_status END) AS score_status,
        MAX(CASE WHEN var_type = 'input' THEN aggregate_status END) AS input_status,
        MAX(CASE WHEN var_type = 'raw' THEN aggregate_status END) AS raw_status,
        MAX(CASE WHEN var_type = 'transformed' THEN aggregate_status END) AS transformed_status,
        MAX(CASE WHEN var_type = 'SYSTEM' THEN aggregate_status END) AS system_status,
        MAX(CASE WHEN execution_status = 'MISSING_DATA' THEN 1 ELSE 0 END) AS has_missing_data,
        COUNT(DISTINCT var_type) AS var_types_evaluated
    FROM (
        SELECT
            a.information_date,
            a.model_id,
            a.var_type,
            a.aggregate_status,
            e.status AS execution_status
        FROM {alert_aggregate} a
        LEFT JOIN {execution_log} e
            ON a.model_id = e.model_id
            AND a.information_date = e.information_date
    ) sub
    GROUP BY information_date, model_id
    """

    spark.sql(semaphore_sql)
    logger.info(f"refreshed view {dashboard_semaphore}")
    spark.sql(summary_sql)
    logger.info(f"refreshed view {dashboard_model_summary}")
