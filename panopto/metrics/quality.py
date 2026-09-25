"""Módulo quality con la(s) clase(s) NullRateMetric, CardinalityRatioMetric, OutlierRateMetric, DominantCategoryRateMetric, CategoryCompositionDriftMetric."""

from typing import Any

import pyspark.sql.functions as F

from panopto.metrics.base import Metric, MetricRegistry


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


class MedianShiftMetric(Metric):
    """Variación relativa de la mediana vs. baseline.

    Alimenta el chequeo "%Raw variables above median variation thresholds"
    del control Pre Scoring oficial.
    """
    name = "median_shift"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        if baseline is None:
            raise ValueError("median_shift requires a baseline dataframe")
        variable = params["variable"]
        current_median = df.approxQuantile(variable, [0.5], 0.01)
        baseline_median = baseline.approxQuantile(variable, [0.5], 0.01)
        current_value = current_median[0] if current_median else None
        baseline_value = baseline_median[0] if baseline_median else None
        if current_value is None or baseline_value is None:
            return self._make_result(0.0, baseline_value, thresholds, **params)
        if baseline_value != 0.0:
            value = abs(current_value - baseline_value) / abs(baseline_value)
        else:
            value = 0.0 if current_value == 0.0 else 1.0
        return self._make_result(value, baseline_value, thresholds, **params)


class PopulationVariationMetric(Metric):
    """Variación de volumen de población respecto al periodo baseline.

    Cubre "Population Scored" (para ``score``) y "%Raw sources with growth
    rt within Thresholds" (para ``raw``/``input``) usando la misma fórmula:
    ``|población_anterior / población_actual - 1|``.
    """
    name = "population_variation"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        if baseline is None:
            raise ValueError("population_variation requires a baseline dataframe")
        current_count = df.count()
        baseline_count = baseline.count()
        if current_count:
            value = abs((baseline_count / current_count) - 1.0)
        else:
            value = 1.0 if baseline_count else 0.0
        return self._make_result(value, float(baseline_count), thresholds, **params)


class CompletenessMetric(Metric):
    """Fracción de fechas esperadas (ventana de observación) sin datos.

    Cubre el chequeo "Observation windows availability" del control Pre
    Scoring oficial.
    """
    name = "completeness"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        expected_dates = params.get("expected_dates") or []
        date_col = params.get("date_column")
        if not expected_dates or not date_col or date_col not in df.columns:
            return self._make_result(0.0, None, thresholds, **params)
        present = {r[date_col] for r in df.select(F.col(date_col)).distinct().collect()}
        missing = [d for d in expected_dates if d not in present]
        value = len(missing) / len(expected_dates)
        return self._make_result(value, None, thresholds, **params)


MetricRegistry.register(NullRateMetric)
MetricRegistry.register(CardinalityRatioMetric)
MetricRegistry.register(OutlierRateMetric)
MetricRegistry.register(DominantCategoryRateMetric)
MetricRegistry.register(CategoryCompositionDriftMetric)
MetricRegistry.register(MedianShiftMetric)
MetricRegistry.register(PopulationVariationMetric)
MetricRegistry.register(CompletenessMetric)
