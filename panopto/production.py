"""Pipeline de producción por modelo y fecha de información.

Contiene la lógica que el DAG ``panopto_production_runner`` ejecuta por modelo
en cada corrida, extraída a módulo para poder reutilizarla en backfills
históricos (``scripts/backfill_history.py``) sin requerir Airflow ni disparar
el despachador de alertas.
"""

from datetime import datetime as dt
from typing import Any, Dict

from panopto.config.tables import PROCESS_CONFIG
from panopto.logging import get_logger

logger = get_logger(__name__)


def has_successful_run(spark: Any, model_id: str, information_date: str) -> bool:
    """Devuelve True si ya existe una corrida SUCCESS para el modelo y fecha."""
    table = PROCESS_CONFIG.execution_log_table
    return spark.sql(f"""
        SELECT 1 FROM {table}
        WHERE model_id = '{model_id}'
          AND information_date = '{information_date}'
          AND status = 'SUCCESS'
        LIMIT 1
    """).count() > 0


def _write_log(
    spark: Any,
    schemas: Any,
    writer: Any,
    *,
    execution_id: str,
    dag_id: str,
    model_id: str,
    information_date: str,
    status: str,
    error_message: str,
    reason: str,
    start: Any,
    variables_expected: int = 0,
    variables_processed: int = 0,
    variables_missing: int = 0,
    metrics_calculated: int = 0,
    metrics_failed: int = 0,
    duration_seconds: int = 0,
) -> None:
    """Persiste una fila en ``panopto_execution_log``."""
    log_df = spark.createDataFrame(
        schemas.normalize_rows(PROCESS_CONFIG.execution_log_table, [{
            "execution_id": execution_id,
            "dag_id": dag_id,
            "airflow_run_id": execution_id,
            "run_date": start,
            "end_date": dt.now(),
            "status": status,
            "error_message": error_message,
            "reason": reason,
            "variables_expected": variables_expected,
            "variables_processed": variables_processed,
            "variables_missing": variables_missing,
            "metrics_calculated": metrics_calculated,
            "metrics_failed": metrics_failed,
            "duration_seconds": duration_seconds,
            "information_date": information_date,
            "model_id": model_id,
        }]),
        schema=schemas.get(PROCESS_CONFIG.execution_log_table),
    )
    writer.write_atomic(
        log_df,
        PROCESS_CONFIG.execution_log_table,
        model_id,
        information_date,
        execution_id,
        partition_cols=["information_date", "model_id"],
    )


def run_model_date(
    spark: Any,
    reader: Any,
    writer: Any,
    schemas: Any,
    aggregator: Any,
    calendar: Any,
    model: Dict[str, Any],
    information_date: str,
    execution_id: str,
    dag_id: str,
    reason: str = "SCHEDULED",
) -> str:
    """Ejecuta el pipeline de producción para un modelo en una ``information_date``.

    Corre ``MetricRunner`` y persiste ``panopto_metric_result``,
    ``panopto_alert_aggregate``, ``panopto_variable_summary``,
    ``panopto_data_availability``, ``panopto_scoring_summary`` y
    ``panopto_execution_log`` (las mismas salidas que el DAG
    ``panopto_production_runner``).

    Devuelve el estatus registrado (``SUCCESS`` o ``MISSING_DATA``).
    Las excepciones distintas de ``MissingDataError`` se propagan después de
    registrar ``FAILED`` en el log de ejecución.
    """
    from panopto.metrics.runner import MetricRunner, MissingDataError
    from panopto.metrics.summary import ScoringSummaryBuilder

    model_id = str(model["model_id"])
    start = dt.now()
    try:
        baseline_days = calendar.previous_business_days(information_date, 2)
        baseline_date = baseline_days[0].isoformat() if baseline_days else None
        runner = MetricRunner(spark, reader, calendar=calendar)
        results = runner.run(model_id, information_date, execution_id, baseline_date=baseline_date)
    except MissingDataError as exc:
        _write_log(
            spark, schemas, writer,
            execution_id=execution_id, dag_id=dag_id, model_id=model_id,
            information_date=information_date, status="MISSING_DATA",
            error_message=str(exc), reason=reason, start=start,
            variables_missing=1, duration_seconds=(dt.now() - start).seconds,
        )
        return "MISSING_DATA"
    except Exception as exc:
        _write_log(
            spark, schemas, writer,
            execution_id=execution_id, dag_id=dag_id, model_id=model_id,
            information_date=information_date, status="FAILED",
            error_message=str(exc), reason=reason, start=start,
        )
        raise exc

    aggregate_alerts = aggregator.aggregate(results)
    metric_rows = []
    for r in results:
        metric_rows.append({
            "execution_id": execution_id,
            "variable": r.variable,
            "var_type": r.var_type,
            "metric_name": r.metric_name,
            "metric_value": r.metric_value,
            "baseline_value": r.baseline_value,
            "threshold_ambar": r.threshold_ambar,
            "threshold_red": r.threshold_red,
            "status": r.status,
            "baseline_process_date": r.baseline_process_date,
            "run_date": r.run_date,
            "dag_id": dag_id,
            "airflow_run_id": execution_id,
            "information_date": information_date,
            "model_id": model_id,
        })
    alert_rows = []
    for a in aggregate_alerts:
        alert_rows.append({
            "execution_id": execution_id,
            "var_type": a.var_type,
            "total_metrics": a.total_metrics,
            "count_ambar": a.count_ambar,
            "count_red": a.count_red,
            "equivalent_yellow": a.equivalent_yellow,
            "stress_ratio": a.stress_ratio,
            "aggregate_status": a.aggregate_status,
            "alert_sent": a.alert_sent,
            "alert_type": a.alert_type,
            "red_equivalent_used": a.red_equivalent,
            "alert_ambar_pct_used": a.alert_ambar_pct,
            "alert_red_pct_used": a.alert_red_pct,
            "run_date": dt.now(),
            "information_date": information_date,
            "model_id": model_id,
        })
    if metric_rows:
        metrics_df = spark.createDataFrame(
            schemas.normalize_rows(PROCESS_CONFIG.metric_result_table, metric_rows),
            schema=schemas.get(PROCESS_CONFIG.metric_result_table),
        )
        writer.write_atomic(metrics_df, PROCESS_CONFIG.metric_result_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
    if alert_rows:
        alerts_df = spark.createDataFrame(
            schemas.normalize_rows(PROCESS_CONFIG.alert_aggregate_table, alert_rows),
            schema=schemas.get(PROCESS_CONFIG.alert_aggregate_table),
        )
        writer.write_atomic(alerts_df, PROCESS_CONFIG.alert_aggregate_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
    if runner.summaries:
        summary_df = spark.createDataFrame(
            schemas.normalize_rows(PROCESS_CONFIG.variable_summary_table, runner.summaries),
            schema=schemas.get(PROCESS_CONFIG.variable_summary_table),
        )
        writer.write_atomic(summary_df, PROCESS_CONFIG.variable_summary_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])

    # Data Availability Monitoring: reutiliza la misma configuración/DataReader del runner.
    try:
        availability_rows = runner.check_data_availability(model_id, information_date)
    except Exception as exc:
        logger.warning(f"data availability check failed for {model_id}/{information_date}: {exc}")
        availability_rows = []
    data_availability_done = bool(availability_rows) and all(
        r["control"] == "Latest Data Updated" for r in availability_rows
    )
    if availability_rows:
        availability_df = spark.createDataFrame(
            schemas.normalize_rows(
                PROCESS_CONFIG.data_availability_table,
                [{**r, "execution_id": execution_id, "run_date": dt.now()} for r in availability_rows],
            ),
            schema=schemas.get(PROCESS_CONFIG.data_availability_table),
        )
        writer.write_atomic(availability_df, PROCESS_CONFIG.data_availability_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])

    # Summary Scoring Monitoring: fila canónica agregada de los MISMOS resultados (sin motor separado).
    scoring_summary_row = ScoringSummaryBuilder.build(
        model_id=model_id,
        model_name=model.get("model_name", model_id),
        information_date=information_date,
        vintage=runner._vintage_for(information_date),
        results=results,
        summaries=runner.summaries,
        data_availability_done=data_availability_done,
    )
    scoring_summary_df = spark.createDataFrame(
        schemas.normalize_rows(
            PROCESS_CONFIG.scoring_summary_table,
            [{**scoring_summary_row, "execution_id": execution_id, "run_date": dt.now()}],
        ),
        schema=schemas.get(PROCESS_CONFIG.scoring_summary_table),
    )
    writer.write_atomic(scoring_summary_df, PROCESS_CONFIG.scoring_summary_table, model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])

    _write_log(
        spark, schemas, writer,
        execution_id=execution_id, dag_id=dag_id, model_id=model_id,
        information_date=information_date, status="SUCCESS",
        error_message="", reason=reason, start=start,
        variables_expected=len({r.variable for r in results}),
        variables_processed=len({r.variable for r in results}),
        metrics_calculated=len(results),
        duration_seconds=(dt.now() - start).seconds,
    )
    return "SUCCESS"
