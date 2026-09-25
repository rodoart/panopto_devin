#!/usr/bin/env python3
"""Ejemplo mínimo de PANOPTO en modo LOCAL.

Construye una SparkSession ``local[*]`` (sin Hive, HDFS ni Kerberos), lee los
CSV de ``samples/sources`` y ejecuta métricas reales del motor
(``panopto.metrics``) comparando cada tabla contra un baseline sintético.

Uso:
    scripts/run_local_demo.sh
    python scripts/local_smoke.py --master local[4] --samples-dir samples/sources

No interviene con el modo cluster: solo usa variables ``PANOPTO_*`` de prueba
y un Spark local efímero.
"""

import argparse
import os
import sys

# Defaults seguros de sandbox (misma idea que tests/conftest.py): sin metastore
# Hive => SparkSessionBuilder arranca en local[*] automáticamente.
os.environ.setdefault("PANOPTO_ENV", "test")
os.environ.setdefault("PANOPTO_HIVE_METASTORE_URIS", "")
os.environ.setdefault("PANOPTO_HIVE_WAREHOUSE_DIR", "/tmp/panopto_local_warehouse")
os.environ.setdefault("PANOPTO_DISABLE_EMAILS", "true")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pyspark.sql.functions as F  # noqa: E402

from panopto.sessions import SparkSessionBuilder  # noqa: E402
from panopto.metrics.quality import (  # noqa: E402
    MedianShiftMetric,
    NullRateMetric,
    PopulationVariationMetric,
)
from panopto.metrics.stability import PSICanonicalMetric  # noqa: E402

PARAMS = {
    "null_rate": {"threshold_ambar": 0.05, "threshold_red": 0.10},
    "median_shift": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "population_variation": {"threshold_ambar": 0.15, "threshold_red": 0.30},
    "psi_canonical": {"threshold_ambar": 0.10, "threshold_red": 0.20},
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ejemplo mínimo PANOPTO en Spark local")
    parser.add_argument("--master", default="local[*]", help="spark.master (default: local[*])")
    parser.add_argument(
        "--samples-dir",
        default=os.path.join(ROOT, "samples", "sources"),
        help="directorio con los CSV de ejemplo",
    )
    parser.add_argument("--model-id", default="local_demo")
    parser.add_argument("--date", default="2025-10-15", help="information_date a evaluar")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    spark = SparkSessionBuilder(
        app_name="panopto-local-smoke",
        extra_conf={
            "spark.master": args.master,
            "spark.sql.shuffle.partitions": "2",
        },
    ).build()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = spark.read.option("header", True).option("inferSchema", True).csv(
            os.path.join(args.samples_dir, "raw_1079.csv")
        )
        score = spark.read.option("header", True).option("inferSchema", True).csv(
            os.path.join(args.samples_dir, "score_1079.csv")
        )

        # Baseline sintético: numéricos x1.5 (drift) y población duplicada, para
        # observar estados AMBAR/RED sin datos reales adicionales.
        raw_baseline = raw.withColumn("mean_var_1_6m", F.col("mean_var_1_6m") * F.lit(1.5))
        raw_doubled = raw.union(
            raw.withColumn("customer_id", F.concat(F.lit("B_"), F.col("customer_id")))
        )

        common = {
            "model_id": args.model_id,
            "information_date": args.date,
            "execution_id": "local_smoke",
            "var_type": "raw",
        }
        checks = [
            ("null_rate", NullRateMetric(), raw, None,
             {"variable": "mean_var_1_6m", "data_type": "numeric"}),
            ("median_shift", MedianShiftMetric(), raw, raw_baseline,
             {"variable": "mean_var_1_6m", "data_type": "numeric"}),
            ("population_variation", PopulationVariationMetric(), raw, raw_doubled,
             {"variable": "mean_var_1_6m", "data_type": "numeric"}),
            ("psi_canonical", PSICanonicalMetric(), score,
             score.withColumn("score", F.col("score") * F.lit(0.6)),
             {"variable": "score", "data_type": "numeric"}),
        ]

        print("\n=== PANOPTO local smoke ===")
        print(f"master={args.master}  model={args.model_id}  date={args.date}\n")
        print(f"{'metric':<22}{'value':>10}{'baseline':>12}  status")
        print("-" * 52)
        for name, metric, current, baseline, extra in checks:
            result = metric.calculate(
                current, baseline, PARAMS[name], **common, **extra
            )
            baseline_value = "-" if result.baseline_value is None else f"{result.baseline_value:.4f}"
            print(f"{result.metric_name:<22}{result.metric_value:>10.4f}{baseline_value:>12}  {result.status}")
        print()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
