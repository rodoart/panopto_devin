"""Backfill histórico: sube historia de un modelo ya dado de alta y entrenado.

Ejecuta exactamente la misma lógica por fecha que el DAG
``panopto_production_runner`` (``panopto.production.run_model_date``):
corre ``MetricRunner`` y persiste ``panopto_metric_result``,
``panopto_alert_aggregate``, ``panopto_variable_summary``,
``panopto_data_availability``, ``panopto_scoring_summary`` y
``panopto_execution_log`` para cada ``information_date`` histórica.

Diferencias con el DAG programado:

- Solo procesa el ``model_id`` indicado (el DAG itera todos los activos).
- No dispara ``panopto_alert_dispatcher``: las corridas históricas no envían
  correos. Las filas quedan con ``reason='BACKFILL'`` y ``dag_id='manual_backfill'``.
- Las fechas candidatas se calculan con el mismo ``BanamexCalendar`` y las
  mismas reglas de frecuencia del DAG (``execution_monthly_day``,
  ``execution_weekday``, ``business_daily``).

Requisitos: el modelo ya debe existir en ``panopto_model_summary_csi_psi`` /
``panopto_model_table_config`` / ``panopto_variable_metadata`` (ver
``scripts/onboard_model.py``) y haber entrenado bins/umbrales
(``panopto_config_watcher`` o ``TrainingMode``).

Uso:
    # últimos 6 meses de historia
    python scripts/backfill_history.py --model-id 1079_cta_lvl --unit months --amount 6

    # últimas 12 semanas, terminando en una fecha distinta a hoy
    python scripts/backfill_history.py --model-id 1079_cta_lvl --unit weeks --amount 12 --end-date 2026-08-07

    # rango explícito
    python scripts/backfill_history.py --model-id 1079_cta_lvl --from 2026-01-01 --to 2026-08-07

    # reprocesar fechas ya exitosas
    python scripts/backfill_history.py --model-id 1079_cta_lvl --unit days --amount 30 --force
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Las dependencias (pyspark, psycopg2) se importan dentro de main() para que
# `--help` funcione también fuera del cluster.


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill histórico de un modelo PANOPTO (misma lógica que panopto_production_runner)",
    )
    parser.add_argument("--model-id", required=True, help="model_id registrado en panopto_model_summary_csi_psi")
    parser.add_argument("--end-date", default=None, help="última fecha a procesar, ISO YYYY-MM-DD (default: hoy)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--amount", type=int, help="cuántas unidades de historia cubrir hacia atrás")
    parser.add_argument(
        "--unit",
        choices=["days", "weeks", "months"],
        default="days",
        help="unidad de --amount (default: days)",
    )
    group.add_argument("--from", dest="from_date", default=None, metavar="YYYY-MM-DD", help="inicio de rango explícito")
    parser.add_argument("--to", dest="to_date", default=None, metavar="YYYY-MM-DD", help="fin de rango explícito (con --from; default: --end-date o hoy)")
    parser.add_argument("--force", action="store_true", help="reprocesar aunque ya exista una corrida SUCCESS")
    return parser.parse_args()


def _daterange(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _candidate_dates(model: Dict[str, Any], calendar: Any, start: date, end: date) -> List[str]:
    """Devuelve las information_date que el DAG habría procesado en [start, end].

    Simula día a día las mismas reglas de ``panopto_production_runner``:
    ``business_daily`` solo corre en días hábiles, y ``weekly``/``monthly``
    solo en la fecha de carga esperada.
    """
    frequency = model.get("frequency", "daily")
    monthly_day = model.get("execution_monthly_day")
    weekday = model.get("execution_weekday")
    dates: List[str] = []
    seen = set()
    for day in _daterange(start, end):
        if frequency == "business_daily" and not calendar.is_business_day(day):
            continue
        info = calendar.expected_information_date(
            frequency,
            day,
            execution_monthly_day=monthly_day,
            execution_weekday=weekday,
        )
        if frequency in ("weekly", "monthly") and info != day.isoformat():
            continue
        if info not in seen:
            seen.add(info)
            dates.append(info)
    return dates


def _resolve_range(args: argparse.Namespace, calendar: Any) -> tuple:
    end: date = datetime.fromisoformat(args.end_date).date() if args.end_date else date.today()
    if args.from_date:
        start = datetime.fromisoformat(args.from_date).date()
        end = datetime.fromisoformat(args.to_date).date() if args.to_date else end
        return start, end
    if args.amount is None or args.amount <= 0:
        raise SystemExit("--amount debe ser un entero positivo")
    if args.unit == "days":
        start = end - timedelta(days=args.amount)
    elif args.unit == "weeks":
        start = end - timedelta(weeks=args.amount)
    else:  # months
        start = datetime.fromisoformat(calendar.shift_months(end, -args.amount)).date()
    return start, end


def main() -> int:
    args = _parse_args()

    from panopto.alerts.aggregator import AlertAggregator
    from panopto.calendar import BanamexCalendar
    from panopto.config.schemas import OutputSchemas
    from panopto.config.tables import PROCESS_CONFIG
    from panopto.data.reader import DataReader
    from panopto.io.atomic_parquet_writer import AtomicParquetWriter
    from panopto.production import has_successful_run, run_model_date
    from panopto.sessions import SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="panopto-backfill").build()
    schemas = OutputSchemas()
    reader = DataReader(spark)
    writer = AtomicParquetWriter(spark)
    aggregator = AlertAggregator()
    calendar = BanamexCalendar()

    model_table = PROCESS_CONFIG.model_summary_table
    model_id = str(args.model_id)
    rows = spark.sql(f"""
        SELECT * FROM {model_table}
        WHERE model_id = '{model_id}'
          AND process_date = (SELECT max(process_date) FROM {model_table} WHERE model_id = '{model_id}')
    """).collect()
    if not rows:
        raise SystemExit(f"model {model_id} not found in {model_table}; onboardee primero con scripts/onboard_model.py")
    model = rows[0].asDict()

    start, end = _resolve_range(args, calendar)
    dates = _candidate_dates(model, calendar, start, end)
    if not dates:
        print(f"sin information_date candidatas para {model_id} en [{start} .. {end}]")
        return 0
    print(f"{len(dates)} information_date(s) a procesar para {model_id} ({model.get('frequency', 'daily')}):")
    for d in dates:
        print(f"  - {d}")

    failed: List[str] = []
    skipped = 0
    for information_date in dates:
        if not args.force and has_successful_run(spark, model_id, information_date):
            print(f"skip {information_date}: ya existe una corrida SUCCESS")
            skipped += 1
            continue
        execution_id = f"backfill_{model_id}_{information_date}_{datetime.now():%Y%m%d%H%M%S}"
        try:
            status = run_model_date(
                spark, reader, writer, schemas, aggregator, calendar,
                model, information_date, execution_id,
                dag_id="manual_backfill", reason="BACKFILL",
            )
            print(f"{information_date}: {status}")
        except Exception as exc:
            failed.append(information_date)
            print(f"{information_date}: FAILED ({exc})")

    print(f"\nbackfill terminado: {len(dates) - skipped - len(failed)} procesadas, {skipped} omitidas, {len(failed)} fallidas")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
