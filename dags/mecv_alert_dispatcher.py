import dataclasses
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def dispatch_alerts(**context):
    from datetime import datetime as dt
    from mecv.alerts.aggregator import AlertAggregator
    from mecv.alerts.dispatcher import EmailDispatcher
    from mecv.calendar import BanamexCalendar
    from mecv.data.reader import DataReader
    from mecv.io.atomic_parquet_writer import AtomicParquetWriter
    from mecv.metrics.runner import MetricRunner, MissingDataError
    from mecv.sessions import SparkSessionBuilder

    today = dt.now().date()
    today_str = today.strftime("%Y-%m-%d")
    execution_id = context["run_id"]
    spark = SparkSessionBuilder(app_name="mecv_alert_dispatcher").build()
    reader = DataReader(spark)
    writer = AtomicParquetWriter(spark)
    aggregator = AlertAggregator()
    dispatcher = EmailDispatcher()
    calendar = BanamexCalendar()
    model_summary = spark.sql("""
        SELECT * FROM model_summary_csi_psi_d_t_d
        WHERE process_date = (SELECT max(process_date) FROM model_summary_csi_psi_d_t_d)
          AND status = 'active'
    """)
    for row in model_summary.collect():
        model_id = str(row.model_id)
        model_name = str(row.model_name)
        try:
            baseline_days = calendar.previous_business_days(today, 2)
            baseline_date = baseline_days[0].isoformat() if baseline_days else None
            runner = MetricRunner(spark, reader, join_keys=["customer_id"])
            results = runner.run(model_id, today_str, execution_id, baseline_date=baseline_date)
            alerts = aggregator.aggregate(results)
            log = dispatcher.dispatch(
                model_id=model_id,
                information_date=today_str,
                aggregate_alerts=alerts,
                metric_results=results,
                model_name=model_name,
                execution_id=execution_id,
            )
            email_row = dataclasses.asdict(log)
            email_row["information_date"] = today_str
            email_row["model_id"] = model_id
            email_df = spark.createDataFrame([email_row])
            writer.write_atomic(email_df, "mecv_email_log_d_t_d", model_id, today_str, execution_id, partition_cols=["information_date", "model_id"])
        except MissingDataError:
            log = dispatcher.dispatch(
                model_id=model_id,
                information_date=today_str,
                aggregate_alerts=[],
                metric_results=[],
                model_name=model_name,
                missing_data=True,
                missing_days=1,
                execution_id=execution_id,
            )
            email_row = dataclasses.asdict(log)
            email_row["information_date"] = today_str
            email_row["model_id"] = model_id
            email_df = spark.createDataFrame([email_row])
            writer.write_atomic(email_df, "mecv_email_log_d_t_d", model_id, today_str, execution_id, partition_cols=["information_date", "model_id"])
        except Exception:
            continue


def handle_missing_data(**context):
    from mecv.sessions import PostgresSession
    today = datetime.now().date()
    psql = PostgresSession()
    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT model_id, count(*) AS missing_days
                FROM mecv_execution_log_d_t_d
                WHERE information_date >= %s - interval '7 days'
                  AND status = 'MISSING_DATA'
                GROUP BY model_id
            """, (today,))
            for row in cur.fetchall():
                # enviar alerta de datos faltantes pendientes
                print(f"missing data for {row[0]}: {row[1]} days")


with DAG(
    "mecv_alert_dispatcher",
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
    dispatch = PythonOperator(task_id="dispatch_emails", python_callable=dispatch_alerts)
    missing = PythonOperator(task_id="handle_missing_data", python_callable=handle_missing_data)
    dispatch >> missing
