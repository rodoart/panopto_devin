import math

import pyspark.sql.functions as F

from mecv.metrics.base import Metric, MetricRegistry


def _is_unbounded(value):
    if value is None:
        return True
    s = str(value).lower()
    return s in ("-inf", "inf", "infinity", "-infinity")


def _bin_condition(df, variable, b):
    bin_type = b.get("bin_type", "NUMERIC")
    if bin_type == "CATEGORICAL":
        return df[variable] == b["category_value"]
    col = F.col(variable)
    cond = F.lit(True)
    lb = b.get("lb")
    ub = b.get("ub")
    lbt = b.get("lower_bound_type", ">")
    ubt = b.get("upper_bound_type", "<=")
    if not _is_unbounded(lb):
        if lbt == ">":
            cond = cond & (col > lb)
        elif lbt == ">=":
            cond = cond & (col >= lb)
        elif lbt == "<":
            cond = cond & (col < lb)
        elif lbt == "<=":
            cond = cond & (col <= lb)
    if not _is_unbounded(ub):
        if ubt == "<":
            cond = cond & (col < ub)
        elif ubt == "<=":
            cond = cond & (col <= ub)
        elif ubt == ">":
            cond = cond & (col > ub)
        elif ubt == ">=":
            cond = cond & (col >= ub)
    return cond


def _psi_value(actual_props, expected_props, eps=1e-6):
    return sum((a - e) * math.log((a + eps) / (e + eps)) for a, e in zip(actual_props, expected_props))


def _psi_from_bins(df, baseline, variable, bins):
    actual_counts = [df.filter(_bin_condition(df, variable, b)).count() for b in bins]
    total_actual = sum(actual_counts) or 1
    actual_props = [c / total_actual for c in actual_counts]
    expected_counts = []
    for b in bins:
        if "count_dev" in b:
            expected_counts.append(b["count_dev"])
        elif baseline is not None:
            expected_counts.append(baseline.filter(_bin_condition(baseline, variable, b)).count())
        else:
            expected_counts.append(0)
    total_expected = sum(expected_counts) or 1
    expected_props = [c / total_expected for c in expected_counts]
    return _psi_value(actual_props, expected_props)


def _dynamic_numeric_bins(baseline, variable, n_bins):
    edges = baseline.approxQuantile(variable, [float(i) / n_bins for i in range(1, n_bins)], 0.01)
    edges = [float("-inf")] + edges + [float("inf")]
    bins = []
    for i in range(len(edges) - 1):
        bins.append({
            "lb": edges[i],
            "ub": edges[i + 1],
            "lower_bound_type": ">",
            "upper_bound_type": "<=",
            "bin_type": "NUMERIC",
        })
    return bins


def _dynamic_categorical_bins(baseline, variable, n_bins):
    rows = baseline.groupBy(F.col(variable)).count().orderBy(F.desc("count")).limit(n_bins).collect()
    bins = []
    for r in rows:
        bins.append({
            "category_value": r[variable],
            "bin_type": "CATEGORICAL",
            "count_dev": r["count"],
        })
    return bins


class PSICanonicalMetric(Metric):
    name = "psi_canonical"

    def calculate(self, df, baseline, thresholds, **params):
        variable = params["variable"]
        bins = params.get("bins", [])
        if not bins and baseline is not None:
            n_bins = params.get("n_bins", 10)
            data_type = params.get("data_type", "numeric")
            bins = (
                _dynamic_categorical_bins(baseline, variable, n_bins)
                if data_type == "categorical"
                else _dynamic_numeric_bins(baseline, variable, n_bins)
            )
        value = _psi_from_bins(df, baseline, variable, bins)
        return self._make_result(value, 0.0, thresholds, **params)


class PSIDynamicMetric(Metric):
    name = "psi_dynamic"

    def calculate(self, df, baseline, thresholds, **params):
        if baseline is None:
            raise ValueError("psi_dynamic requires a baseline dataframe")
        variable = params["variable"]
        n_bins = params.get("n_bins", 10)
        data_type = params.get("data_type", "numeric")
        bins = (
            _dynamic_categorical_bins(baseline, variable, n_bins)
            if data_type == "categorical"
            else _dynamic_numeric_bins(baseline, variable, n_bins)
        )
        value = _psi_from_bins(df, baseline, variable, bins)
        return self._make_result(value, 0.0, thresholds, **params)


class KSMetric(Metric):
    name = "ks_vs_dev"

    def calculate(self, df, baseline, thresholds, **params):
        if baseline is None:
            raise ValueError("ks_vs_dev requires a baseline dataframe")
        variable = params["variable"]
        total_current = df.count() or 1
        total_baseline = baseline.count() or 1
        points = df.union(baseline).approxQuantile(variable, [float(i) / 20 for i in range(21)], 0.01)
        ks = 1e-9
        for p in points:
            fc = df.filter(F.col(variable) <= p).count() / total_current
            fb = baseline.filter(F.col(variable) <= p).count() / total_baseline
            ks = max(ks, abs(fc - fb))
        return self._make_result(ks, 0.0, thresholds, **params)


class CorrelationDriftMetric(Metric):
    name = "correlation_drift"

    def _max_abs_corr(self, df):
        cols = [c for c, t in df.dtypes if t in ("double", "float", "int", "bigint", "long", "short", "tinyint")]
        maximum = 1e-9
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                c = df.stat.corr(cols[i], cols[j])
                if c is not None:
                    maximum = max(maximum, abs(c))
        return maximum

    def calculate(self, df, baseline, thresholds, **params):
        current = self._max_abs_corr(df)
        baseline_value = None
        if baseline is not None:
            baseline_value = self._max_abs_corr(baseline)
            value = abs(current - baseline_value)
        else:
            value = current
        return self._make_result(value, baseline_value, thresholds, **params)


MetricRegistry.register(PSICanonicalMetric)
MetricRegistry.register(PSIDynamicMetric)
MetricRegistry.register(KSMetric)
MetricRegistry.register(CorrelationDriftMetric)
