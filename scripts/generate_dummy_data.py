#!/usr/bin/env python3
"""Genera data dummy mensual para un modelo PANOPTO en el ambiente LOCAL.

Crea (en el metastore local, sin HDFS) las tablas fuente de un modelo mensual y
sus filas de configuración, listas para el flujo completo
(``scripts/run_local_pipeline.py``):

- ``raw_<model>``: variables crudas de negocio (numéricas y categóricas).
- ``input_<model>``: features del modelo (numéricas).
- ``transformed_<model>``: variables transformadas del score.
- ``score_<model>``: score (probabilidad) del modelo.
- ``target_<model>``: target binaria por cohorte con ``target_lag`` meses de
  desfase: la partición del mes ``m`` contiene el desempeño de la cohorte
  scoreada en ``m`` (observado en ``m + target_lag``). Se materializa desde
  ``start - target_lag`` para que exista historia al evaluar los últimos meses.

El mes más antiguo (``--start-month``) queda reservado para el entrenamiento
(``TrainingMode``); los ``months - 1`` siguientes se corren en producción.

También siembra en PostgreSQL el calendario ``banamex_calendar_sync_d``
(Lun–Vie hábiles) y un contacto en ``model_contact``.

Uso:
    python scripts/generate_dummy_data.py --model-id local_monthly_001
    python scripts/generate_dummy_data.py --months 12 --clients 500 --seed 7 --drift
"""

import argparse
import calendar as calmod
import os
import sys
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
import local_env  # noqa: E402  (debe ejecutarse antes de importar panopto)

local_env.apply()

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

REGIONS = ["NORTE", "CENTRO", "SUR", "OCCIDENTE"]
SEGMENTS = ["A", "B", "C"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generador de data dummy mensual PANOPTO (local)")
    parser.add_argument("--model-id", default="local_monthly_001")
    parser.add_argument("--clients", type=int, default=400, help="clientes por mes")
    parser.add_argument("--months", type=int, default=12,
                        help="meses de historia (el primero es para entrenar)")
    parser.add_argument("--start-month", default=None,
                        help="primer mes YYYY-MM (default: months atrás desde el mes actual)")
    parser.add_argument("--execution-day", type=int, default=5,
                        help="día de ejecución mensual (information_date = primer hábil >= día)")
    parser.add_argument("--target-lag", type=int, default=2,
                        help="desfase de la target en meses")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--drift", action="store_true",
                        help="inyecta drift gradual y un pico de nulos en los últimos meses")
    return parser.parse_args()


def first_business_on_or_after(year: int, month: int, day: int) -> date:
    """Primer día hábil (Lun–Vie) en o después de ``year-month-day``."""
    last = calmod.monthrange(year, month)[1]
    d = date(year, month, min(day, last))
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def shift_months(d: date, months: int) -> date:
    """Desplaza ``d`` por ``months`` meses (día acotado a 28)."""
    total = d.year * 12 + d.month - 1 + months
    return date(total // 12, total % 12 + 1, min(d.day, 28))


def month_range(start: date, months: int) -> List[Tuple[int, int]]:
    """Lista de (year, month) desde ``start`` (primer día) hacia adelante."""
    out = []
    y, m = start.year, start.month
    for _ in range(months):
        out.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def info_dates(start_month: date, months: int, execution_day: int) -> List[str]:
    """information_date de cada mes: primer hábil >= execution_day."""
    return [
        first_business_on_or_after(y, m, execution_day).isoformat()
        for y, m in month_range(start_month, months)
    ]


def _build_month(
    rng: np.random.Generator,
    info_date: str,
    customer_ids: np.ndarray,
    month_idx: int,
    total_months: int,
    drift: bool,
) -> Dict[str, pd.DataFrame]:
    """Genera las 5 tablas fuente de un mes a partir del mismo latente de riesgo."""
    n = len(customer_ids)
    drift_factor = 1.0 + 0.06 * max(0, month_idx - (total_months - 4)) if drift else 1.0

    income = rng.lognormal(mean=np.log(25000), sigma=0.5, size=n) * drift_factor
    age = np.clip(rng.normal(40, 12, n), 18, 90)
    balance = rng.normal(50000 * drift_factor, 15000, n)
    txn = rng.poisson(25, n).astype(float)
    # Pico de nulos en un mes intermedio para ver AMBAR en el tablero.
    null_rate = 0.08 if (drift and month_idx == total_months - 3) else 0.01
    balance = np.where(rng.random(n) < null_rate, np.nan, balance)

    # Drift categórico en los últimos meses: crece el segmento C.
    seg_p = np.array([0.3, 0.35, 0.35]) if (drift and month_idx >= total_months - 2) else np.array([0.5, 0.35, 0.15])
    region = rng.choice(REGIONS, size=n)
    segment = rng.choice(SEGMENTS, size=n, p=seg_p)

    # Latente de riesgo -> score (probabilidad) -> target Bernoulli del cohort.
    region_risk = pd.Series(region).map({"NORTE": 0.2, "CENTRO": 0.0, "SUR": -0.1, "OCCIDENTE": 0.1}).to_numpy()
    seg_risk = pd.Series(segment).map({"A": -0.4, "B": 0.0, "C": 0.5}).to_numpy()
    z = (
        -2.0
        + 0.00001 * income
        - np.nan_to_num(balance) / 500000.0
        + 0.02 * txn / 25
        + region_risk
        + seg_risk
        + rng.normal(0, 0.5, n)
    )
    score = 1.0 / (1.0 + np.exp(-z))
    target = (rng.random(n) < score).astype(int)

    base = {"customer_id": customer_ids, "information_date": info_date}
    raw = pd.DataFrame({
        **base,
        "raw_income": np.round(income, 2),
        "raw_age": np.round(age, 1),
        "raw_balance": np.round(balance, 2),
        "raw_txn_count": txn,
        "raw_region": region,
        "raw_segment": segment,
    })
    inp = pd.DataFrame({
        **base,
        "in_income_log": np.round(np.log1p(income), 4),
        "in_balance_ratio": np.round(np.nan_to_num(balance) / (income + 1.0), 4),
        "in_txn_freq": np.round(txn / 30.0, 4),
        "in_region_woe": np.round(region_risk, 4),
    })
    transformed = pd.DataFrame({
        **base,
        "tr_risk_score_lin": np.round(z, 4),
        "tr_score_decile": pd.qcut(score, 10, labels=False, duplicates="drop").astype(float) + 1.0,
    })
    score_df = pd.DataFrame({**base, "score": np.round(score, 6)})
    target_df = pd.DataFrame({**base, "target": target})
    return {"raw": raw, "input": inp, "transformed": transformed, "score": score_df, "target": target_df}


def seed_postgres_calendar(psql, start: date, end: date) -> int:
    """Siembra ``banamex_calendar_sync_d`` (Lun–Vie hábiles) en el rango dado."""
    rows = []
    d = start
    while d <= end:
        rows.append((d, d.weekday() < 5, False, None, pd.Timestamp.now()))
        d += timedelta(days=1)
    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO banamex_calendar_sync_d"
                " (calendar_date, is_business_day, is_holiday, holiday_name, sync_timestamp)"
                " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (calendar_date) DO NOTHING",
                rows,
            )
        conn.commit()
    return len(rows)


def seed_postgres_contact(psql, model_id: str, process_date: str) -> None:
    with psql.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO model_contact"
                " (model_id, contact_email, contact_role, notify_on_ambar, notify_on_red, notify_on_missing, process_date)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                " ON CONFLICT (model_id, contact_email, process_date) DO NOTHING",
                (model_id, "local-owner@example.com", "owner", True, True, True, process_date),
            )
        conn.commit()


def _config_rows(args) -> Tuple[dict, List[dict], List[dict], List[dict], List[dict]]:
    """Filas de configuración del modelo (misma forma que onboard_model.py)."""
    roles = [
        ("raw", f"raw_{args.model_id}", 0),
        ("input", f"input_{args.model_id}", 0),
        ("transformed", f"transformed_{args.model_id}", 0),
        ("score", f"score_{args.model_id}", 0),
        ("target", f"target_{args.model_id}", args.target_lag),
    ]
    table_rows = [{
        "table_role": role,
        "table_name": table,
        "source_type": "HIVE",
        "source_schema": "",
        "source_table": table,
        "entity_key_columns": '["customer_id"]',
        "canonical_key_columns": '["customer_id"]',
        "date_column": "information_date",
        "date_format": "",
        "history_months": 1,
        "lag": lag,
        "sql_transform": "",
        "data_type": "",
        "partition_columns": '["information_date"]',
        # Las fuentes dummy son snapshots mensuales (1 partición/mes):
        # "each" esperaría una partición por día hábil -> completeness ~0.95.
        "reading_mode": "first_partition",
        "use_business_days": True,
        "active": True,
        "deadline_days": 10,
    } for role, table, lag in roles]

    variables = [
        ("raw", "numeric", "raw_income"), ("raw", "numeric", "raw_age"),
        ("raw", "numeric", "raw_balance"), ("raw", "numeric", "raw_txn_count"),
        ("raw", "categorical", "raw_region"), ("raw", "categorical", "raw_segment"),
        ("input", "numeric", "in_income_log"), ("input", "numeric", "in_balance_ratio"),
        ("input", "numeric", "in_txn_freq"), ("input", "numeric", "in_region_woe"),
        ("transformed", "numeric", "tr_risk_score_lin"),
        ("transformed", "numeric", "tr_score_decile"),
        ("score", "numeric", "score"),
        ("target", "binary", "target"),
    ]
    var_rows = [{
        "variable": var,
        "var_type": var_type,
        "data_type": data_type,
        "source_table": f"{var_type}_{args.model_id}",
        "source_column": var,
        "is_monotonic": var_type == "score",
    } for var_type, data_type, var in variables]

    model_row = {
        "model_name": "Modelo mensual local (dummy)",
        "model_description": "Modelo sintético de riesgo mensual para demo local",
        "model_type": "binary",
        "status": "active",
        "cut_off_probability": 0.5,
        "frequency": "monthly",
        "window_value": 1,
        "window_unit": "months",
        "execution_monthly_day": args.execution_day,
        "execution_weekday": None,
        "trigger_csi_ambar": 0.1,
        "trigger_csi_red": 0.2,
        "trigger_csi_variation_ambar": 0.05,
        "trigger_csi_variation_red": 0.1,
        "score_alert_ambar_pct": 0.3,
        "score_alert_red_pct": 0.15,
        "score_red_equivalent": 2,
    }

    policy_rows = [
        {"variable": "raw_region", "top_n_threshold": 50, "critical_top_k": 5},
        {"variable": "raw_segment", "top_n_threshold": 50, "critical_top_k": 5},
    ]
    return model_row, table_rows, var_rows, policy_rows, []


def generate(
    model_id: str,
    clients: int = 400,
    months: int = 12,
    start_month: Optional[date] = None,
    execution_day: int = 5,
    target_lag: int = 2,
    seed: int = 42,
    drift: bool = False,
    spark: Optional[object] = None,
) -> Tuple[str, List[str]]:
    """Genera la data y config del modelo. Devuelve (process_date, info_dates)."""
    from panopto.sessions import PostgresSession, SparkSessionBuilder
    from local_stack import create_panopto_tables, insert_config_rows, spark_conf

    if start_month is None:
        today = date.today().replace(day=1)
        start_month = shift_months(today, -months)
    dates = info_dates(start_month, months, execution_day)
    # Particiones de la target: cohortes desde start-lag hasta el último mes
    # (la cohorte c se observa en c+lag; el run de t lee la partición t-lag).
    target_dates = info_dates(shift_months(start_month, -target_lag), months + target_lag, execution_day)

    rng = np.random.default_rng(seed)
    customer_ids = np.array([f"C{i:06d}" for i in range(1, clients + 1)])
    total_months = months + target_lag
    month_frames = [
        _build_month(rng, d, customer_ids, idx, total_months, drift)
        for idx, d in enumerate(target_dates)
    ]

    own_spark = spark is None
    if own_spark:
        spark = SparkSessionBuilder(app_name="panopto-local-dummy", extra_conf=spark_conf()).build()
        spark.sparkContext.setLogLevel("WARN")
    try:
        create_panopto_tables(spark)
        prod_set = set(dates)
        for role in ("raw", "input", "transformed", "score", "target"):
            pdf_list = [
                mf[role] for mf in month_frames
                if role == "target" or mf["raw"]["information_date"].iloc[0] in prod_set
            ]
            table = f"{role}_{model_id}"
            sdf = spark.createDataFrame(pd.concat(pdf_list, ignore_index=True))
            sdf.write.mode("overwrite").partitionBy("information_date").saveAsTable(table)
            print(f"[seed] {table}: {sdf.count()} filas, {sdf.select('information_date').distinct().count()} particiones")

        args_like = argparse.Namespace(
            model_id=model_id, execution_day=execution_day, target_lag=target_lag
        )
        model_row, table_rows, var_rows, policy_rows, threshold_rows = _config_rows(args_like)
        insert_config_rows(
            spark, model_id, dates[0], model_row, table_rows, var_rows, policy_rows, threshold_rows
        )

        psql = PostgresSession()
        n_cal = seed_postgres_calendar(
            psql,
            shift_months(start_month, -target_lag) - timedelta(days=5),
            shift_months(start_month, months + 2),
        )
        seed_postgres_contact(psql, model_id, dates[0])
        print(f"[seed] banamex_calendar_sync_d: {n_cal} días")

        print("\n=== Data dummy lista ===")
        print(f"model_id        : {model_id}")
        print(f"train month     : {dates[0]} (process_date de la config)")
        print(f"production dates: {dates[1]} .. {dates[-1]} ({len(dates) - 1} corridas)")
        print(f"target cohorts  : {target_dates[0]} .. {target_dates[-1]} (lag={target_lag})")
        return dates[0], dates
    finally:
        if own_spark:
            spark.stop()


def main() -> int:
    args = _parse_args()
    start = None
    if args.start_month:
        y, m = map(int, args.start_month.split("-"))
        start = date(y, m, 1)
    generate(
        model_id=args.model_id,
        clients=args.clients,
        months=args.months,
        start_month=start,
        execution_day=args.execution_day,
        target_lag=args.target_lag,
        seed=args.seed,
        drift=args.drift,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
