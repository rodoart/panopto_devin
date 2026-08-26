import pyspark.sql.functions as F
from pyspark.sql import Window

from mecv.metrics.base import Metric, MetricRegistry
from mecv.metrics.stability import _psi_from_bins


def _entropy(df, score_col):
    total = df.count()
    if total == 0:
        return 0.0
    bin_col = F.least(F.floor(F.col(score_col) * 10), F.lit(9)).alias("bin")
    return df.withColumn("bin", bin_col).groupBy("bin").count().withColumn(
        "p", F.col("count") / total
    ).agg(F.sum(-F.col("p") * F.log(F.col("p"))).alias("entropy")).collect()[0]["entropy"] or 0.0


class ScoreRangeMetric(Metric):
    name = "range_violation"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        total = df.count()
        violations = df.filter(
            (F.col(score_col) < 0.0) | (F.col(score_col) > 1.0)
        ).count()
        value = (violations / total) if total else 0.0
        return self._make_result(value, None, thresholds, **params)


class EntropyMetric(Metric):
    name = "entropy"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        current = _entropy(df, score_col)
        baseline_value = None
        if baseline is not None:
            baseline_value = _entropy(baseline, score_col)
            if baseline_value != 0.0:
                value = abs(current - baseline_value) / baseline_value
            else:
                value = 0.0
        else:
            value = current
        return self._make_result(value, baseline_value, thresholds, **params)


class ApprovalRateMetric(Metric):
    name = "approval_rate"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        cutoff = params.get("cut_off_probability", 0.5)
        total = df.count()
        current_rate = df.filter(F.col(score_col) > cutoff).count() / total if total else 0.0
        baseline_value = None
        if baseline is not None:
            b_total = baseline.count()
            baseline_value = baseline.filter(F.col(score_col) > cutoff).count() / b_total if b_total else 0.0
            if baseline_value != 0.0:
                value = abs(current_rate - baseline_value) / baseline_value
            else:
                value = 1.0 if current_rate > 0 else 0.0
        else:
            value = current_rate
        return self._make_result(value, baseline_value, thresholds, **params)


class ScoreTailShiftMetric(Metric):
    name = "tail_shift"

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        if baseline is None:
            raise ValueError("tail_shift requires a baseline dataframe")
        current_p10 = df.approxQuantile(score_col, [0.1], 0.01)[0]
        current_p90 = df.approxQuantile(score_col, [0.9], 0.01)[0]
        baseline_p10 = baseline.approxQuantile(score_col, [0.1], 0.01)[0]
        baseline_p90 = baseline.approxQuantile(score_col, [0.9], 0.01)[0]
        value = abs(current_p10 - baseline_p10) + abs(current_p90 - baseline_p90)
        return self._make_result(value, 0.0, thresholds, **params)


class ConcentrationGiniMetric(Metric):
    name = "concentration_gini"

    def _gini(self, df, score_col):
        n = df.count()
        if n < 2:
            return 0.0
        df2 = df.withColumn("rk", F.row_number().over(Window.orderBy(F.col(score_col))))
        r = df2.agg(
            F.sum(F.col(score_col) * F.col("rk")).alias("num"),
            F.sum(F.col(score_col)).alias("den"),
        ).collect()[0]
        if not r or r["den"] is None or r["den"] == 0:
            return 0.0
        return (2.0 * r["num"] / (n * r["den"])) - ((n + 1.0) / n)

    def calculate(self, df, baseline, thresholds, **params):
        score_col = params.get("score_col", "score")
        current = self._gini(df, score_col)
        baseline_value = self._gini(baseline, score_col) if baseline is not None else None
        if baseline_value is not None and baseline_value != 0.0:
            value = abs(current - baseline_value) / abs(baseline_value)
        else:
            value = abs(current)
        return self._make_result(value, baseline_value, thresholds, **params)


class PSIApprovedMetric(Metric):
    name = "psi_approved"

    def calculate(self, df, baseline, thresholds, **params):
        if baseline is None:
            raise ValueError("psi_approved requires a baseline dataframe")
        score_col = params.get("score_col", "score")
        cutoff = params.get("cut_off_probability", 0.5)
        bins = params.get("bins", [])
        if not bins:
            raise ValueError("psi_approved requires bins")
        approved = df.filter(F.col(score_col) > cutoff)
        approved_baseline = baseline.filter(F.col(score_col) > cutoff)
        value = _psi_from_bins(approved, approved_baseline, score_col, bins)
        return self._make_result(value, 0.0, thresholds, **params)


class PSIRejectedMetric(Metric):
    name = "psi_rejected"

    def calculate(self, df, baseline, thresholds, **params):
        if baseline is None:
            raise ValueError("psi_rejected requires a baseline dataframe")
        score_col = params.get("score_col", "score")
        cutoff = params.get("cut_off_probability", 0.5)
        bins = params.get("bins", [])
        if not bins:
            raise ValueError("psi_rejected requires bins")
        rejected = df.filter(F.col(score_col) <= cutoff)
        rejected_baseline = baseline.filter(F.col(score_col) <= cutoff)
        value = _psi_from_bins(rejected, rejected_baseline, score_col, bins)
        return self._make_result(value, 0.0, thresholds, **params)


MetricRegistry.register(ScoreRangeMetric)
MetricRegistry.register(EntropyMetric)
MetricRegistry.register(ApprovalRateMetric)
MetricRegistry.register(ScoreTailShiftMetric)
MetricRegistry.register(ConcentrationGiniMetric)
MetricRegistry.register(PSIApprovedMetric)
MetricRegistry.register(PSIRejectedMetric)
