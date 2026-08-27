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
        total = df.count()
        nulls = df.select(
            F.sum(F.when(F.col(variable).isNull(), 1).otherwise(0)).alias("n")
        ).collect()[0]["n"]
        value = (nulls / total) if total else 0.0
        baseline_value = None
        if baseline is not None:
            b_total = baseline.count()
            b_nulls = baseline.select(
                F.sum(F.when(F.col(variable).isNull(), 1).otherwise(0)).alias("n")
            ).collect()[0]["n"]
            baseline_value = (b_nulls / b_total) if b_total else 0.0
        return self._make_result(value, baseline_value, thresholds, **params)


class CardinalityRatioMetric(Metric):
    """Clase que representa CardinalityRatioMetric."""
    name = "cardinality_ratio"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        total = df.count()
        distinct = df.select(F.countDistinct(F.col(variable)).alias("d")).collect()[0]["d"]
        value = (distinct / total) if total else 0.0
        return self._make_result(value, None, thresholds, **params)


class OutlierRateMetric(Metric):
    """Clase que representa OutlierRateMetric."""
    name = "outlier_rate"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        total = df.count()
        row = df.select(
            F.percentile_approx(F.col(variable), 0.25).alias("q1"),
            F.percentile_approx(F.col(variable), 0.75).alias("q3"),
        ).collect()[0]
        if row is None or row[0] is None or row[1] is None:
            return self._make_result(0.0, None, thresholds, **params)
        q1, q3 = row[0], row[1]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = df.filter(
            (F.col(variable) < lower) | (F.col(variable) > upper)
        ).count()
        value = (outliers / total) if total else 0.0
        return self._make_result(value, None, thresholds, **params)


class DominantCategoryRateMetric(Metric):
    """Clase que representa DominantCategoryRateMetric."""
    name = "dominant_category_rate"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        variable = params["variable"]
        total = df.count()
        max_freq = df.groupBy(F.col(variable)).count().agg(F.max("count").alias("m")).collect()[0]["m"]
        max_freq = max_freq or 0
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
