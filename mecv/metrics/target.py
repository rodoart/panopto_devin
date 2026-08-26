import pyspark.sql.functions as F

from mecv.metrics.base import Metric, MetricRegistry
from mecv.metrics.stability import _psi_from_bins


class EventRateMetric(Metric):
    name = "event_rate"

    def calculate(self, df, baseline, thresholds, **params):
        target_col = params.get("target_col", "target")
        current = df.select(F.mean(F.col(target_col)).alias("m")).collect()[0]["m"]
        current = current or 0.0
        baseline_value = None
        if baseline is not None:
            baseline_value = baseline.select(F.mean(F.col(target_col)).alias("m")).collect()[0]["m"]
            baseline_value = baseline_value or 0.0
            if baseline_value != 0.0:
                value = abs(current - baseline_value) / baseline_value
            else:
                value = 1.0 if current > 0 else 0.0
        else:
            value = current
        return self._make_result(value, baseline_value, thresholds, **params)


class PSITargetMetric(Metric):
    name = "psi_target"

    def calculate(self, df, baseline, thresholds, **params):
        target_col = params.get("target_col", "target")
        bins = params.get("bins", [])
        if not bins:
            current_cats = [r[target_col] for r in df.select(target_col).distinct().collect()]
            bins = [{"category_value": c, "bin_type": "CATEGORICAL"} for c in current_cats]
        value = _psi_from_bins(df, baseline, target_col, bins)
        return self._make_result(value, 0.0, thresholds, **params)


MetricRegistry.register(EventRateMetric)
MetricRegistry.register(PSITargetMetric)
