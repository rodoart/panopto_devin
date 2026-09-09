"""Módulo quality con la(s) clase(s) NullRateMetric, CardinalityRatioMetric, OutlierRateMetric, DominantCategoryRateMetric, CategoryCompositionDriftMetric."""

from typing import Any

import pyspark.sql.functions as F

from mecv.metrics.base import Metric, MetricRegistry


class NullRateMetric(Metric):
    """Clase que representa NullRateMetric."""
    name = "null_rate"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        row = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col(variable).isNull(), 1).otherwise(0)).alias("nulls"),
        ).collect()[0]
        total = row["total"] or 0
        nulls = row["nulls"] or 0
        value = (nulls / total) if total else 0.0
        baseline_value = None
        if baseline is not None:
            b_row = baseline.agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(F.col(variable).isNull(), 1).otherwise(0)).alias("nulls"),
            ).collect()[0]
            b_total = b_row["total"] or 0
            b_nulls = b_row["nulls"] or 0
            baseline_value = (b_nulls / b_total) if b_total else 0.0
        return self._make_result(value, baseline_value, thresholds, **params)


class CardinalityRatioMetric(Metric):
    """Clase que representa CardinalityRatioMetric."""
    name = "cardinality_ratio"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        row = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.countDistinct(F.col(variable)).alias("distinct"),
        ).collect()[0]
        total = row["total"] or 0
        distinct = row["distinct"] or 0
        value = (distinct / total) if total else 0.0
        return self._make_result(value, None, thresholds, **params)


class OutlierRateMetric(Metric):
    """Clase que representa OutlierRateMetric."""
    name = "outlier_rate"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        col = F.col(variable)
        row = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.percentile_approx(col, 0.25).alias("q1"),
            F.percentile_approx(col, 0.75).alias("q3"),
        ).collect()[0]
        if row is None or row["q1"] is None or row["q3"] is None:
            return self._make_result(0.0, None, thresholds, **params)
        total = row["total"] or 0
        q1, q3 = row["q1"], row["q3"]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        if total == 0:
            return self._make_result(0.0, None, thresholds, **params)
        out_row = df.agg(
            F.sum(F.when((col < lower) | (col > upper), 1).otherwise(0)).alias("outliers")
        ).collect()[0]
        outliers = out_row["outliers"] or 0
        value = outliers / total
        return self._make_result(value, None, thresholds, **params)


class DominantCategoryRateMetric(Metric):
    """Clase que representa DominantCategoryRateMetric."""
    name = "dominant_category_rate"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        row = (
            df.groupBy(F.col(variable))
            .count()
            .agg(
                F.sum("count").alias("total"),
                F.max("count").alias("max_freq"),
            )
            .collect()[0]
        )
        total = row["total"] or 0
        max_freq = row["max_freq"] or 0
        value = (max_freq / total) if total else 0.0
        return self._make_result(value, None, thresholds, **params)


class CategoryCompositionDriftMetric(Metric):
    """Clase que representa CategoryCompositionDriftMetric."""
    name = "category_composition_drift"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        top_n = params.get("top_n", 10)
        current_top = df.groupBy(F.col(variable)).count().orderBy(F.desc("count")).limit(top_n).collect()
        current_cats = {r[variable] for r in current_top}
        baseline_cats = set()
        if baseline is not None:
            baseline_top = baseline.groupBy(F.col(variable)).count().orderBy(F.desc("count")).limit(top_n).collect()
            baseline_cats = {r[variable] for r in baseline_top}
        union = current_cats | baseline_cats
        intersection = current_cats & baseline_cats
        jaccard = (len(intersection) / len(union)) if union else 1.0
        value = 1.0 - jaccard
        return self._make_result(value, jaccard, thresholds, **params)


MetricRegistry.register(NullRateMetric)
MetricRegistry.register(CardinalityRatioMetric)
MetricRegistry.register(OutlierRateMetric)
MetricRegistry.register(DominantCategoryRateMetric)
MetricRegistry.register(CategoryCompositionDriftMetric)
