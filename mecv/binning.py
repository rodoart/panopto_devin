"""Módulo binning con las funciones numeric_bins, categorical_bins, compute_bin_counts, compute_woe."""

from typing import Any, Dict, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def numeric_bins(df: DataFrame, variable: str, n_bins: int = 10) -> List[Dict[str, Any]]:
    """Función que realiza la operación "numeric_bins"."""
    edges = df.approxQuantile(variable, [float(i) / n_bins for i in range(1, n_bins)], 0.01)
    edges = [float("-inf")] + edges + [float("inf")]
    bins = []
    for i in range(len(edges) - 1):
        bins.append({
            "bin": i + 1,
            "bin_type": "NUMERIC",
            "lb": edges[i],
            "ub": edges[i + 1],
            "lower_bound_type": ">",
            "upper_bound_type": "<=",
        })
    return bins


def categorical_bins(df: DataFrame, variable: str, top_n: int = 50) -> List[Dict[str, Any]]:
    """Función que realiza la operación "categorical_bins"."""
    rows = df.groupBy(F.col(variable)).count().orderBy(F.desc("count")).limit(top_n).collect()
    bins = []
    for i, r in enumerate(rows):
        bins.append({
            "bin": i + 1,
            "bin_type": "CATEGORICAL",
            "category_value": r[variable],
            "count_dev": r["count"],
        })
    return bins


def compute_bin_counts(df: DataFrame, variable: str, bins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Función que calcula bin counts."""
    from mecv.metrics.stability import _bin_condition
    total = df.count() or 1
    for b in bins:
        b["count_dev"] = df.filter(_bin_condition(df, variable, b)).count()
        b["freq_dev"] = b["count_dev"] / total
    return bins


def compute_woe(bins: List[Dict[str, Any]], positive: int, negative: int, eps: float = 1e-6) -> List[Dict[str, Any]]:
    """Función que calcula woe."""
    total_pos = positive or 1
    total_neg = negative or 1
    for b in bins:
        p = (b.get("count_pos", 0) + eps) / total_pos
        n = (b.get("count_neg", 0) + eps) / total_neg
        b["woe"] = float("inf") if n <= 0 else float("-inf") if p <= 0 else round(float(100.0 * (p / n)), 6)
    return bins
