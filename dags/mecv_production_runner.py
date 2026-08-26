from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from mecv.logging import get_logger

logger = get_logger(__name__)


def run_production(**context):
    from datetime import datetime as dt
    from mecv.alerts.aggregator import AlertAggregator
    from mecv.calendar import BanamexCalendar
    from mecv.data.reader import DataReader
    from mecv.io.atomic_parquet_writer import AtomicParquetWriter
    from mecv.metrics.runner import MetricRunner, MissingDataError
    from mecv.sessions import PostgresSession, SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="mecv_production_runner").build()
    reader = DataReader(spark)
    psql = PostgresSession()
    writer = AtomicParquetWriter(spark)
    aggregator = AlertAggregator()
    calendar = BanamexCalendar()
    today = dt.now().date()
    today_str = today.strftime("%Y-%m-%d")
    execution_id = context["run_id"]
    logger.info(f"starting production run for {today_str}")
    dag_id = context["dag"]["dag_id"]

    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT is_business_day FROM banamex_calendar_sync_d WHERE calendar_date = %s", (today,))
            row = cur.fetchone()
            is_business = row[0] if row else True

    model_summary = spark.sql("""
        SELECT * FROM model_summary_csi_psi_d_t_d
        WHERE process_date = (SELECT max(process_date) FROM model_summary_csi_psi_d_t_d)
          AND status = 'active'
    """)
    models = [r.asDict() for r in model_summary.collect()]

    for model in models:
        model_id = str(model["model_id"])
        frequency = model.get("frequency", "daily")
        information_date = calendar.expected_information_date(frequency, today)
        logger.info(f"processing model {model_id} with frequency {frequency}, information_date {information_date}")
        if frequency == "business_daily" and not is_business:
            continue
        if frequency in ("weekly", "monthly") and information_date != today_str:
            continue
        start = dt.now()
        try:
            baseline_days = calendar.previous_business_days(information_date, 2)
            baseline_date = baseline_days[0].isoformat() if baseline_days else None
            runner = MetricRunner(spark, reader, join_keys=["customer_id"], calendar=calendar)
            results = runner.run(model_id, information_date, execution_id, baseline_date=baseline_date)
        except MissingDataError as exc:
            log_df = spark.createDataFrame([{
                "execution_id": execution_id,
                "dag_id": dag_id,
                "airflow_run_id": execution_id,
                "run_date": start,
                "end_date": dt.now(),
                "status": "MISSING_DATA",
                "error_message": str(exc),
                "reason": "SCHEDULED",
                "variables_expected": 0,
                "variables_processed": 0,
                "variables_missing": 1,
                "metrics_calculated": 0,
                "metrics_failed": 0,
                "duration_seconds": (dt.now() - start).seconds,
                "information_date": information_date,
                "model_id": model_id,
            }])
            writer.write_atomic(log_df, "mecv_execution_log_d_t_d", model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
            continue
        except Exception as exc:
            log_df = spark.createDataFrame([{
                "execution_id": execution_id,
                "dag_id": dag_id,
                "airflow_run_id": execution_id,
                "run_date": start,
                "end_date": dt.now(),
                "status": "FAILED",
                "error_message": str(exc),
                "reason": "SCHEDULED",
                "variables_expected": 0,
                "variables_processed": 0,
                "variables_missing": 0,
                "metrics_calculated": 0,
                "metrics_failed": 0,
                "duration_seconds": 0,
                "information_date": information_date,
                "model_id": model_id,
            }])
            writer.write_atomic(log_df, "mecv_execution_log_d_t_d", model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
            continue

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
            metrics_df = spark.createDataFrame(metric_rows)
            writer.write_atomic(metrics_df, "mecv_metric_result_d_t_d", model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
        if alert_rows:
            alerts_df = spark.createDataFrame(alert_rows)
            writer.write_atomic(alerts_df, "mecv_alert_aggregate_d_t_d", model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
        if runner.summaries:
            summary_df = spark.createDataFrame(runner.summaries)
            writer.write_atomic(summary_df, "mecv_variable_summary_d_t_d", model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])
        log_df = spark.createDataFrame([{
            "execution_id": execution_id,
            "dag_id": dag_id,
            "airflow_run_id": execution_id,
            "run_date": start,
            "end_date": dt.now(),
            "status": "SUCCESS",
            "error_message": "",
            "reason": "SCHEDULED",
            "variables_expected": len({r.variable for r in results}),
            "variables_processed": len({r.variable for r in results}),
            "variables_missing": 0,
            "metrics_calculated": len(results),
            "metrics_failed": 0,
            "duration_seconds": (dt.now() - start).seconds,
            "information_date": information_date,
            "model_id": model_id,
        }])
        writer.write_atomic(log_df, "mecv_execution_log_d_t_d", model_id, information_date, execution_id, partition_cols=["information_date", "model_id"])


with DAG(
    "mecv_production_runner",
    default_args={
        "owner": "mecv",
        "start_date": datetime(2025, 10, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@daily",
    catchup=False,
    tags=["mecv"],
) as dag:
    run = PythonOperator(task_id="run_production", python_callable=run_production)
    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_alert_dispatcher",
        trigger_dag_id="mecv_alert_dispatcher",
        conf={"information_date": "{{ ds }}"},
    )
    run >> trigger_alert
