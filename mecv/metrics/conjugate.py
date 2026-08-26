import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from mecv.metrics.base import Metric, MetricRegistry
from mecv.metrics.common import binary_auc, binary_gini


class AUCMetric(Metric):
    name = "auc"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        value = binary_auc(df, score_col, target_col)
        baseline_value = None
        if baseline is not None:
            baseline_value = binary_auc(baseline, score_col, target_col)
        return self._make_result(value, baseline_value, thresholds, **params)


class GiniMetric(Metric):
    name = "gini"

    def calculate(self, df, baseline, thresholds, **params):
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
    name = "brier_score"

    def calculate(self, df, baseline, thresholds, **params):
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
    name = "lift_top_decile"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        decile = F.ntile(10).over(Window.orderBy(F.desc(score_col)))
        current_dec = df.withColumn("decile", decile)
        overall = df.agg(F.mean(target_col).alias("m")).collect()[0]["m"]
        top = current_dec.filter(F.col("decile") == 1).agg(F.mean(target_col).alias("m")).collect()[0]["m"]
        current_lift = (top / overall) if overall else 0.0
        baseline_value = None
        if baseline is not None:
            b_decile = F.ntile(10).over(Window.orderBy(F.desc(score_col)))
            baseline_dec = baseline.withColumn("decile", b_decile)
            b_overall = baseline.agg(F.mean(target_col).alias("m")).collect()[0]["m"]
            b_top = baseline_dec.filter(F.col("decile") == 1).agg(F.mean(target_col).alias("m")).collect()[0]["m"]
            baseline_value = (b_top / b_overall) if b_overall else 0.0
            if baseline_value != 0.0:
                value = (baseline_value - current_lift) / baseline_value
            else:
                value = 0.0
        else:
            value = current_lift
        return self._make_result(value, baseline_value, thresholds, **params)


class CalibrationSlopeMetric(Metric):
    name = "calibration_slope"

    def _slope(self, df, score_col, target_col):
        df_ml = df.select(F.col(score_col).cast("double").alias(score_col), F.col(target_col).cast("double").alias(target_col)).dropna()
        if df_ml.count() < 2:
            return 1.0
        assembler = VectorAssembler(inputCols=[score_col], outputCol="features", handleInvalid="skip")
        vec = assembler.transform(df_ml)
        lr = LinearRegression(featuresCol="features", labelCol=target_col, fitIntercept=True, regParam=1e-12)
        model = lr.fit(vec)
        return float(model.coefficients[0])

    def calculate(self, df, baseline, thresholds, **params):
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
    name = "ks_score_target"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        target_col = params.get("target_col", "target")
        df0 = df.filter(F.col(target_col) == 0).select(score_col)
        df1 = df.filter(F.col(target_col) == 1).select(score_col)
        n0 = df0.count() or 1
        n1 = df1.count() or 1
        points = df0.union(df1).approxQuantile(score_col, [float(i) / 20 for i in range(21)], 0.01)
        ks = 1e-9
        for p in points:
            f0 = df0.filter(F.col(score_col) <= p).count() / n0
            f1 = df1.filter(F.col(score_col) <= p).count() / n1
            ks = max(ks, abs(f0 - f1))
        baseline_value = None
        if baseline is not None:
            b0 = baseline.filter(F.col(target_col) == 0).select(score_col)
            b1 = baseline.filter(F.col(target_col) == 1).select(score_col)
            bn0 = b0.count() or 1
            bn1 = b1.count() or 1
            ks_baseline = 1e-9
            b_points = b0.union(b1).approxQuantile(score_col, [float(i) / 20 for i in range(21)], 0.01)
            for p in b_points:
                f0 = b0.filter(F.col(score_col) <= p).count() / bn0
                f1 = b1.filter(F.col(score_col) <= p).count() / bn1
                ks_baseline = max(ks_baseline, abs(f0 - f1))
            baseline_value = ks_baseline
            value = abs(ks - ks_baseline) / ks_baseline if ks_baseline else ks
        else:
            value = ks
        return self._make_result(value, baseline_value, thresholds, **params)


MetricRegistry.register(AUCMetric)
MetricRegistry.register(GiniMetric)
MetricRegistry.register(BrierScoreMetric)
MetricRegistry.register(LiftTopDecileMetric)
MetricRegistry.register(CalibrationSlopeMetric)
MetricRegistry.register(KSScoreTargetMetric)
