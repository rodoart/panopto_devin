"""Módulo conjugate con la(s) clase(s) AUCMetric, GiniMetric, BrierScoreMetric, LiftTopDecileMetric, CalibrationSlopeMetric, KSScoreTargetMetric."""

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from mecv.metrics.base import Metric, MetricRegistry
from mecv.metrics.common import binary_auc, binary_gini


class AUCMetric(Metric):
    """Clase que representa AUCMetric."""
    name = "auc"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        value = binary_auc(df, score_col, target_col)
        baseline_value = None
        if baseline is not None:
            baseline_value = binary_auc(baseline, score_col, target_col)
        return self._make_result(value, baseline_value, thresholds, **params)


class GiniMetric(Metric):
    """Clase que representa GiniMetric."""
    name = "gini"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        current_gini = binary_gini(df, score_col, target_col)
        baseline_value = None
        if baseline is not None:
            baseline_value = binary_gini(baseline, score_col, target_col)
            if baseline_value != 0.0:
                value = (baseline_value - current_gini) / baseline_value
            else:
                value = 0.0
        else:
            value = current_gini
        return self._make_result(value, baseline_value, thresholds, **params)


class BrierScoreMetric(Metric):
    """Clase que representa BrierScoreMetric."""
    name = "brier_score"

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        current_brier = df.select(
            F.mean(F.pow(F.col(score_col) - F.col(target_col), 2)).alias("brier")
        ).collect()[0]["brier"]
        baseline_value = None
        if baseline is not None:
            baseline_value = baseline.select(
                F.mean(F.pow(F.col(score_col) - F.col(target_col), 2)).alias("brier")
            ).collect()[0]["brier"]
            if baseline_value and baseline_value != 0.0:
                value = (current_brier - baseline_value) / baseline_value
            else:
                value = 0.0
        else:
            value = current_brier
        return self._make_result(value, baseline_value, thresholds, **params)


class LiftTopDecileMetric(Metric):
    """Clase que representa LiftTopDecileMetric."""
    name = "lift_top_decile"

    def _lift(self, df: Any, score_col: Any, target_col: Any) -> float:
        """Cálculo de lift top decil en una sola pasada."""
        decile = F.ntile(10).over(Window.orderBy(F.desc(score_col)))
        dec_df = df.withColumn("decile", decile)
        row = dec_df.agg(
            F.mean(F.col(target_col)).alias("overall"),
            F.avg(F.when(F.col("decile") == 1, F.col(target_col))).alias("top"),
        ).collect()[0]
        overall = row["overall"]
        top = row["top"]
        return (top / overall) if overall else 0.0

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        current_lift = self._lift(df, score_col, target_col)
        baseline_value = None
        if baseline is not None:
            baseline_value = self._lift(baseline, score_col, target_col)
            if baseline_value != 0.0:
                value = (baseline_value - current_lift) / baseline_value
            else:
                value = 0.0
        else:
            value = current_lift
        return self._make_result(value, baseline_value, thresholds, **params)


class CalibrationSlopeMetric(Metric):
    """Clase que representa CalibrationSlopeMetric."""
    name = "calibration_slope"

    def _slope(self, df: Any, score_col: Any, target_col: Any) -> Any:
        """Helper interno que realiza la operación "slope"."""
        df_ml = df.select(F.col(score_col).cast("double").alias(score_col), F.col(target_col).cast("double").alias(target_col)).dropna()
        if df_ml.count() < 2:
            return 1.0
        assembler = VectorAssembler(inputCols=[score_col], outputCol="features", handleInvalid="skip")
        vec = assembler.transform(df_ml)
        lr = LinearRegression(featuresCol="features", labelCol=target_col, fitIntercept=True, regParam=1e-12)
        model = lr.fit(vec)
        return float(model.coefficients[0])

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        slope = self._slope(df, score_col, target_col)
        baseline_value = None
        if baseline is not None:
            baseline_value = self._slope(baseline, score_col, target_col)
            value = abs(slope - baseline_value) / abs(baseline_value) if baseline_value else abs(slope - 1.0)
        else:
            value = abs(slope - 1.0)
        return self._make_result(value, baseline_value, thresholds, **params)


class KSScoreTargetMetric(Metric):
    """Clase que representa KSScoreTargetMetric."""
    name = "ks_score_target"

    def _ks(self, df: Any, score_col: Any, target_col: Any) -> float:
        """Cálculo de KS en una sola pasada por cada clase."""
        df0 = df.filter(F.col(target_col) == 0).select(score_col)
        df1 = df.filter(F.col(target_col) == 1).select(score_col)
        points = df0.union(df1).approxQuantile(score_col, [float(i) / 20 for i in range(21)], 0.01)
        col = F.col(score_col)
        c0_exprs = [F.sum(F.when(col <= p, 1).otherwise(0)).alias(f"c_{i}") for i, p in enumerate(points)]
        c1_exprs = [F.sum(F.when(col <= p, 1).otherwise(0)).alias(f"c_{i}") for i, p in enumerate(points)]
        c0_row = df0.agg(F.count(F.lit(1)).alias("n"), *c0_exprs).collect()[0]
        c1_row = df1.agg(F.count(F.lit(1)).alias("n"), *c1_exprs).collect()[0]
        n0 = c0_row["n"] or 1
        n1 = c1_row["n"] or 1
        ks = 1e-9
        for i, p in enumerate(points):
            f0 = (c0_row[f"c_{i}"] or 0) / n0
            f1 = (c1_row[f"c_{i}"] or 0) / n1
            ks = max(ks, abs(f0 - f1))
        return float(ks)

    def calculate(self, df: Any, baseline: Any, thresholds: Any, **params: Any) -> Any:
        """Método que calcula."""
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        ks = self._ks(df, score_col, target_col)
        baseline_value = None
        value = ks
        if baseline is not None:
            baseline_value = self._ks(baseline, score_col, target_col)
            value = abs(ks - baseline_value) / baseline_value if baseline_value else ks
        return self._make_result(value, baseline_value, thresholds, **params)


MetricRegistry.register(AUCMetric)
MetricRegistry.register(GiniMetric)
MetricRegistry.register(BrierScoreMetric)
MetricRegistry.register(LiftTopDecileMetric)
MetricRegistry.register(CalibrationSlopeMetric)
MetricRegistry.register(KSScoreTargetMetric)
