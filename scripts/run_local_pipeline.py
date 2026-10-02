#!/usr/bin/env python3
"""Orquesta el flujo PANOPTO completo en el ambiente LOCAL (Windows).

Replica la cadena de DAGs del cluster sin Airflow (Airflow no corre nativo en
Windows; en el cluster la orquestación la hacen los DAGs ``panopto_*``):

    config_watcher  ->  TrainingMode (mes más antiguo = entrenamiento)
    production      ->  run_model_date por cada information_date posterior
    output_validator->  build_dashboard_views + sync a PostgreSQL
    dashboard       ->  streamlit run panopto/dashboard/app.py

Subcomandos:
    postgres   arranca el Postgres portable (.local/pgsql) si no está vivo
    tables     crea las tablas panopto_* en el metastore local
    generate   genera la data dummy + config + calendario (generate_dummy_data)
    train      entrena con el mes más antiguo (TrainingMode)
    produce    corre run_model_date para los meses 2..N
    views      reconstruye las vistas del dashboard (builder.py)
    sync       sincroniza Hive local -> PostgreSQL (pg_sync)
    dashboard  levanta streamlit (http://localhost:8501)
    all        todo lo anterior en un solo proceso (default)

Uso:
    python scripts/run_local_pipeline.py all --clients 400 --drift
    python scripts/run_local_pipeline.py produce --model-id local_monthly_001
"""

import argparse
import os
import subprocess
import sys
from datetime import date
from typing import List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
import local_env  # noqa: E402  (debe ejecutarse antes de importar panopto)

local_env.apply()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline local PANOPTO (sin Airflow)")
    parser.add_argument("command", nargs="?", default="all",
                        choices=["postgres", "tables", "generate", "train", "produce",
                                 "views", "sync", "repair", "dashboard", "all"])
    parser.add_argument("--model-id", default="local_monthly_001")
    parser.add_argument("--clients", type=int, default=400)
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--start-month", default=None, help="YYYY-MM")
    parser.add_argument("--execution-day", type=int, default=5)
    parser.add_argument("--target-lag", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--drift", action="store_true")
    parser.add_argument("--port", type=int, default=8501)
    return parser.parse_args()


def _pg_ctl(*args: str) -> subprocess.CompletedProcess:
    pg_ctl = local_env.ROOT / ".local" / "pgsql" / "bin" / "pg_ctl.exe"
    if not pg_ctl.exists():
        pg_ctl = local_env.ROOT / ".local" / "pgsql" / "bin" / "pg_ctl"
    return subprocess.run(
        [str(pg_ctl), "-D", str(local_env.ROOT / ".local" / "pgdata"), *args],
        capture_output=True, text=True,
    )


def cmd_postgres() -> None:
    status = _pg_ctl("status")
    if status.returncode == 0:
        print("[postgres] ya está corriendo")
        return
    log = local_env.ROOT / ".local" / "pg.log"
    port = os.environ.get("PANOPTO_POSTGRES_PORT", "55432")
    r = _pg_ctl("-l", str(log), "-o", f"-p {port}", "start")
    print(r.stdout.strip() or r.stderr.strip())


def _build_spark():
    from panopto.sessions import SparkSessionBuilder
    from local_stack import spark_conf

    spark = SparkSessionBuilder(
        app_name="panopto-local-pipeline", extra_conf=spark_conf()
    ).build()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cmd_tables(spark) -> None:
    from local_stack import create_panopto_tables

    created = create_panopto_tables(spark)
    print(f"[tables] {len(created)} tablas panopto_* verificadas/creadas")


def cmd_generate(args, spark) -> Tuple[str, List[str]]:
    from generate_dummy_data import generate, shift_months

    start = None
    if args.start_month:
        y, m = map(int, args.start_month.split("-"))
        start = date(y, m, 1)
    elif args.months:
        start = shift_months(date.today().replace(day=1), -args.months)
    return generate(
        model_id=args.model_id,
        clients=args.clients,
        months=args.months,
        start_month=start,
        execution_day=args.execution_day,
        target_lag=args.target_lag,
        seed=args.seed,
        drift=args.drift,
        spark=spark,
    )


def _info_dates(args) -> List[str]:
    """Reproduce las information_date usadas por ``generate`` (mismo seed de params)."""
    from generate_dummy_data import info_dates, shift_months

    if args.start_month:
        y, m = map(int, args.start_month.split("-"))
        start = date(y, m, 1)
    else:
        start = shift_months(date.today().replace(day=1), -args.months)
    return info_dates(start, args.months, args.execution_day)


def cmd_train(args, spark) -> None:
    from panopto.data.reader import DataReader
    from panopto.training import TrainingMode

    dates = _info_dates(args)
    train_date = dates[0]
    ok = TrainingMode(spark, DataReader(spark)).run(
        args.model_id,
        process_date=train_date,
        execution_id=f"local_train_{train_date}",
        information_date=train_date,
    )
    print(f"[train] modelo {args.model_id} entrenado con {train_date}: {'ok' if ok else 'sin datos'}")


def cmd_produce(args, spark) -> None:
    import pyspark.sql.functions as F
    from panopto.alerts.aggregator import AlertAggregator
    from panopto.calendar import BanamexCalendar
    from panopto.config.schemas import OutputSchemas
    from panopto.config.tables import PROCESS_CONFIG
    from panopto.data.reader import DataReader
    from panopto.io.atomic_parquet_writer import AtomicParquetWriter
    from panopto.production import run_model_date

    reader = DataReader(spark)
    writer = AtomicParquetWriter(spark)
    schemas = OutputSchemas()
    aggregator = AlertAggregator()
    calendar = BanamexCalendar()

    model_df = spark.table(PROCESS_CONFIG.model_summary_table)
    max_pd = model_df.filter(F.col("model_id") == args.model_id).agg(F.max("process_date").alias("m")).collect()[0]["m"]
    model = model_df.filter(
        (F.col("model_id") == args.model_id) & (F.col("process_date") == max_pd)
    ).collect()[0].asDict()

    for info_date in _info_dates(args)[1:]:
        status = run_model_date(
            spark, reader, writer, schemas, aggregator, calendar,
            model, info_date, f"local_{info_date}", "local_pipeline",
        )
        print(f"[produce] {args.model_id} @ {info_date}: {status}")


def cmd_views(spark) -> None:
    from panopto.dashboard.builder import build_dashboard_views

    build_dashboard_views(spark)
    print("[views] vistas del dashboard reconstruidas")


def cmd_repair(spark) -> None:
    """MSCK REPAIR TABLE sobre todas las tablas del metastore local."""
    tables = [r["tableName"] for r in spark.sql("SHOW TABLES").collect()]
    for t in tables:
        try:
            spark.sql(f"MSCK REPAIR TABLE {t}")
        except Exception:
            pass
    print(f"[repair] {len(tables)} tablas reparadas")


def cmd_sync(spark, model_id: Optional[str] = None) -> None:
    from panopto.dashboard.pg_sync import sync_dashboard
    from panopto.sessions import PostgresSession

    sync_dashboard(spark, PostgresSession(), model_id=model_id)
    print("[sync] tablas del dashboard sincronizadas a PostgreSQL")


def cmd_dashboard(args) -> None:
    venv_python = local_env.ROOT / ".venv-local" / "Scripts" / "python.exe"
    if not venv_python.exists():
        venv_python = local_env.ROOT / ".venv-local" / "bin" / "python"
    app = local_env.ROOT / "panopto" / "dashboard" / "app.py"
    url = f"http://localhost:{args.port}"
    print(f"[dashboard] streamlit -> {url} (Ctrl+C para detener)")
    subprocess.run([
        str(venv_python), "-m", "streamlit", "run", str(app),
        "--server.port", str(args.port), "--server.headless", "true",
    ], cwd=str(local_env.ROOT), env=os.environ.copy())


def main() -> int:
    args = _parse_args()
    cmd = args.command

    if cmd in ("postgres", "all"):
        cmd_postgres()
    if cmd in ("postgres", "dashboard"):
        if cmd == "dashboard":
            cmd_dashboard(args)
        return 0

    spark = _build_spark()
    try:
        if cmd in ("tables", "all"):
            cmd_tables(spark)
        if cmd in ("generate", "all"):
            cmd_generate(args, spark)
        if cmd in ("train", "all"):
            cmd_train(args, spark)
        if cmd in ("produce", "all"):
            cmd_produce(args, spark)
        if cmd in ("views", "all"):
            cmd_views(spark)
        if cmd == "repair":
            cmd_repair(spark)
        if cmd in ("sync", "all"):
            cmd_sync(spark, args.model_id)
        if cmd == "all":
            print(f"\nFlujo completo terminado. Dashboard: python scripts/run_local_pipeline.py dashboard --port {args.port}")
            print(f"O directo: .venv-local/Scripts/python -m streamlit run panopto/dashboard/app.py")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
