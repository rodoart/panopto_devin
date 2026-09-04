"""Módulo runner con la(s) clase(s) MissingDataError, MetricRunner."""

import dataclasses
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from mecv.calendar import BanamexCalendar
from mecv.checkpoint import Checkpoint
from mecv.data.reader import DataReader
from mecv.data.sources import DataSourceSpec
from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger
from mecv.metrics.base import MetricRegistry
from mecv.metrics.result import MetricResult
from mecv.metrics.summary import VariableSummaryBuilder

logger = get_logger(__name__)


class MissingDataError(Exception):
    """Clase que representa MissingDataError."""
    pass


DEFAULT_THRESHOLDS = {
    "null_rate": {"threshold_ambar": 0.05, "threshold_red": 0.10},
    "cardinality_ratio": {"threshold_red": 0.95},
    "outlier_rate": {"threshold_ambar": 0.03, "threshold_red": 0.06},
    "dominant_category_rate": {"threshold_red": 0.90},
    "category_composition_drift": {"threshold_ambar": 0.10, "threshold_red": 0.30},
    "psi_canonical": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "psi_dynamic": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "psi_target": {"threshold_ambar": 0.05, "threshold_red": 0.10},
    "range_violation": {"threshold_red": 1e-9},
    "entropy": {"threshold_ambar": 0.15, "threshold_red": 0.30},
    "approval_rate": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "tail_shift": {"threshold_ambar": 0.05, "threshold_red": 0.10},
    "gini": {"threshold_ambar": 0.05, "threshold_red": 0.10},
    "brier_score": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "lift_top_decile": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "event_rate": {"threshold_ambar": 0.20, "threshold_red": 0.40},
    "ks_vs_dev": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "concentration_gini": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "psi_approved": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "psi_rejected": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "calibration_slope": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "ks_score_target": {"threshold_ambar": 0.05, "threshold_red": 0.10},
    "correlation_drift": {"threshold_ambar": 0.10, "threshold_red": 0.20},
    "auc": {},
}

RELATIVE_METRICS = {
    "gini",
    "brier_score",
    "lift_top_decile",
    "entropy",
    "approval_rate",
    "event_rate",
    "concentration_gini",
    "calibration_slope",
    "correlation_drift",
}

NEEDS_BASELINE_DATA = {
    "psi_dynamic",
    "tail_shift",
    "ks_vs_dev",
    "concentration_gini",
    "psi_approved",
    "psi_rejected",
    "calibration_slope",
    "ks_score_target",
    "correlation_drift",
}


class MetricRunner:
    """Clase que representa MetricRunner."""
    def __init__(
        self,
        spark: SparkSession,
        data_reader: DataReader,
        join_keys: Optional[List[str]] = None,
        calendar: Optional[BanamexCalendar] = None,
        checkpoint: Optional[Checkpoint] = None,
    ) -> None:
        """Inicializa una nueva instancia de MetricRunner."""
        self.spark = spark
        self.data_reader = data_reader
        self.join_keys = join_keys or ["customer_id"]
        self.calendar = calendar or BanamexCalendar()
        self.checkpoint = checkpoint or Checkpoint(spark)
        self.summaries = []

    def run(
        self,
        model_id: str,
        information_date: str,
        execution_id: str,
        baseline_date: Optional[str] = None,
    ) -> List[MetricResult]:
        """Método que ejecuta."""
        model_id = str(model_id)
        information_date = str(information_date)
        logger.info(f"running metrics for model {model_id}, information_date {information_date}")
        model_summary = self._load_model_summary(model_id)
        checkpoint_key = self._checkpoint_key(model_id, information_date, baseline_date, model_summary)
        if self.checkpoint.exists(checkpoint_key, "results") and self.checkpoint.exists(checkpoint_key, "summaries"):
            logger.info(f"metric results found in checkpoint for {model_id}/{information_date}; skipping computation")
            results = self._load_results_from_checkpoint(checkpoint_key, execution_id)
            self.summaries = self._load_summaries_from_checkpoint(checkpoint_key, execution_id)
            return results
        variables = self._load_variable_metadata(model_id)
        thresholds_table = self._load_thresholds_table(model_id)
        metric_threshold_auto = self._load_metric_threshold_auto(model_id)
        category_policy = self._load_category_policy(model_id)
        csi_bins = self._load_csi_bins(model_id)

        cut_off = model_summary.get("cut_off_probability", 0.5)
        results: List[MetricResult] = []
        score_df = None
        target_df = None
        score_baseline = None
        target_baseline = None
        score_col_name = None
        target_col_name = None
        input_numeric = []

        frequency = str(model_summary.get("frequency", "daily"))
        for row in variables:
            var = row["variable"]
            var_type = row["var_type"]
            data_type = row["data_type"]
            reading_mode = str(row.get("reading_mode", "each"))
            info_col = row["information_date_column"]
            spec = DataSourceSpec.from_metadata(
                source_table=row["source_table"],
                source_column=row["source_column"],
                information_date_column=info_col,
                partition_columns=row["partition_columns"],
            )
            current_dates, baseline_dates = self._resolve_dates(
                information_date, baseline_date, reading_mode, frequency
            )
            current, baseline = self._read_data(spec, var, current_dates, baseline_dates)
            if current.count() == 0:
                raise MissingDataError(f"no data for {var} on {information_date}")
            self.summaries.extend(
                VariableSummaryBuilder.build(
                    current,
                    var,
                    var_type,
                    data_type,
                    model_id,
                    information_date,
                    execution_id,
                )
            )
            if var_type in ("raw", "input") and data_type == "numeric":
                input_numeric.append((var, current, baseline))
            if var_type == "score":
                score_df = current
                score_baseline = baseline
                score_col_name = var
            if var_type == "target":
                target_df = current
                target_baseline = baseline
                target_col_name = var
            metrics = self._metrics_for_variable(var_type, data_type)
            for metric_name in metrics:
                if metric_name in NEEDS_BASELINE_DATA and baseline is None:
                    continue
                has_baseline = baseline is not None
                thresholds = self._thresholds_for(
                    var,
                    var_type,
                    metric_name,
                    thresholds_table,
                    metric_threshold_auto,
                    has_baseline,
                )
                params = {
                    "model_id": model_id,
                    "information_date": information_date,
                    "execution_id": execution_id,
                    "variable": var,
                    "var_type": var_type,
                    "data_type": data_type,
                    "cut_off_probability": cut_off,
                    "n_bins": 10,
                }
                if var_type == "score":
                    params["score_col"] = var
                if var_type == "target":
                    params["target_col"] = var
                if metric_name in ("psi_canonical", "psi_target", "psi_approved", "psi_rejected"):
                    params["bins"] = csi_bins.get((var, var_type), [])
                if metric_name == "category_composition_drift":
                    params["top_n"] = category_policy.get(var, {}).get("top_n_threshold", 10)
                metric_cls = MetricRegistry.get(metric_name)
                try:
                    res = metric_cls().calculate(current, baseline, thresholds, **params)
                    results.append(res)
                except Exception as exc:
                    logger.warning(f"metric {metric_name} failed for {var}: {exc}")
                    continue

        if score_df is not None and target_df is not None and score_col_name and target_col_name:
            joined = self._join_conjugate(score_df, target_df, score_col_name, target_col_name)
            baseline_joined = None
            if score_baseline is not None and target_baseline is not None:
                baseline_joined = self._join_conjugate(score_baseline, target_baseline, score_col_name, target_col_name)
            for metric_name in ("auc", "gini", "brier_score", "lift_top_decile", "calibration_slope", "ks_score_target"):
                thresholds = self._thresholds_for(
                    "__SCORE__",
                    "score",
                    metric_name,
                    {},
                    metric_threshold_auto,
                    baseline_joined is not None,
                )
                params = {
                    "model_id": model_id,
                    "information_date": information_date,
                    "execution_id": execution_id,
                    "variable": "__SCORE__",
                    "var_type": "conjugate",
                    "data_type": "numeric",
                    "score_col": "score",
                    "target_col": "target",
                }
                metric_cls = MetricRegistry.get(metric_name)
                try:
                    res = metric_cls().calculate(joined, baseline_joined, thresholds, **params)
                    results.append(res)
                except Exception as exc:
                    logger.warning(f"conjugate metric {metric_name} failed: {exc}")
                    continue

        if len(input_numeric) >= 2:
            joined_current = self._join_inputs([df for _, df, _ in input_numeric])
            joined_baseline = None
            if all(b is not None for _, _, b in input_numeric):
                joined_baseline = self._join_inputs([b for _, _, b in input_numeric])
            thresholds = self._thresholds_for(
                "__INPUTS__",
                "input",
                "correlation_drift",
                {},
                metric_threshold_auto,
                joined_baseline is not None,
            )
            params = {
                "model_id": model_id,
                "information_date": information_date,
                "execution_id": execution_id,
                "variable": "__INPUTS__",
                "var_type": "input",
                "data_type": "numeric",
            }
            try:
                metric_cls = MetricRegistry.get("correlation_drift")
                res = metric_cls().calculate(joined_current, joined_baseline, thresholds, **params)
                results.append(res)
            except Exception:
                pass

        if results:
            results_df = self._results_to_df(results)
            self.checkpoint.write(results_df, checkpoint_key, "results")
        if self.summaries:
            summaries_df = self._summaries_to_df(self.summaries)
            self.checkpoint.write(summaries_df, checkpoint_key, "summaries")

        return results

    def _join_inputs(self, dfs: Any) -> Any:
        """Helper interno que une inputs."""
        joined = dfs[0]
        join_cols = [c for c in self.join_keys if c in joined.columns]
        for df in dfs[1:]:
            on = [c for c in join_cols if c in df.columns]
            joined = joined.join(df, on=on, how="inner")
        return joined

    def _load_model_summary(self, model_id: str) -> Dict:
        """Helper interno que carga model summary."""
        df = self._latest_partition(PROCESS_CONFIG.model_summary_table, model_id)
        rows = df.collect()
        return rows[0].asDict() if rows else {}

    def _load_variable_metadata(self, model_id: str) -> List[Dict]:
        """Helper interno que carga variable metadata."""
        df = self._latest_partition(PROCESS_CONFIG.variable_metadata_table, model_id)
        return [r.asDict() for r in df.collect()]

    def _load_thresholds_table(self, model_id: str) -> Dict:
        """Helper interno que carga thresholds table."""
        df = self._latest_partition(PROCESS_CONFIG.thresholds_table, model_id)
        out = {}
        for r in df.collect():
            out[(r["variable"], r["type"])] = r.asDict()
        return out

    def _load_metric_threshold_auto(self, model_id: str) -> Dict:
        """Helper interno que carga metric threshold auto."""
        df = self._latest_partition(PROCESS_CONFIG.metric_threshold_auto_table, model_id)
        out = {}
        for r in df.collect():
            out[(r["variable"], r["metric_name"])] = r.asDict()
        return out

    def _load_category_policy(self, model_id: str) -> Dict:
        """Helper interno que carga category policy."""
        df = self._latest_partition(PROCESS_CONFIG.category_policy_table, model_id)
        out = {}
        for r in df.collect():
            out[r["variable"]] = r.asDict()
        return out

    def _load_csi_bins(self, model_id: str) -> Dict:
        """Helper interno que carga csi bins."""
        df = self._latest_partition(PROCESS_CONFIG.csi_psi_table, model_id)
        out = defaultdict(list)
        for r in df.collect():
            d = r.asDict()
            out[(d["variable"], d["type"])].append(d)
        return dict(out)

    def _latest_partition(self, table: str, model_id: str) -> Any:
        """Helper interno que realiza la operación "latest_partition"."""
        return self.spark.sql(f"""
            SELECT * FROM {table}
            WHERE process_date = (
                SELECT max(process_date) FROM {table} WHERE model_id = '{model_id}'
            ) AND model_id = '{model_id}'
        """)

    def _read_data(self, spec: DataSourceSpec, variable: str, current_dates: List[str], baseline_dates: Optional[List[str]] = None) -> Tuple[Any, ...]:
        """Helper interno que lee data."""
        current = self.data_reader.read(spec, current_dates, extra_cols=self.join_keys)
        current = current.withColumnRenamed(spec.column, variable)
        baseline = None
        if baseline_dates:
            baseline = self.data_reader.read(spec, baseline_dates, extra_cols=self.join_keys)
            baseline = baseline.withColumnRenamed(spec.column, variable)
        return current, baseline

    def _period_dates(self, reference_date: str, reading_mode: str, frequency: str) -> List[str]:
        """Helper interno que realiza la operación "period_dates"."""
        period = "month" if frequency == "monthly" else "week" if frequency == "weekly" else "day"
        if reading_mode == "each" or period == "day":
            return [reference_date]
        if reading_mode == "first":
            return [self.calendar.first_business_day_of_period(reference_date, period)]
        if reading_mode == "last":
            return [self.calendar.last_business_day_of_period(reference_date, period)]
        return [reference_date]

    def _previous_period_reference(self, reference_date: str, frequency: str) -> str:
        """Helper interno que realiza la operación "previous_period_reference"."""
        d = datetime.fromisoformat(reference_date).date()
        if frequency == "monthly":
            if d.month == 1:
                d = d.replace(year=d.year - 1, month=12)
            else:
                d = d.replace(month=d.month - 1)
        elif frequency == "weekly":
            d = d - timedelta(days=7)
        else:
            d = d - timedelta(days=1)
        return d.isoformat()

    def _resolve_dates(self, information_date: str, baseline_date: Optional[str], reading_mode: str, frequency: str) -> Tuple[Any, ...]:
        """Helper interno que resuelve dates."""
        current_dates = self._period_dates(information_date, reading_mode, frequency)
        if reading_mode == "each":
            if baseline_date:
                baseline_dates = [baseline_date]
            else:
                prev = self.calendar.previous_business_days(information_date, 1)
                baseline_dates = [prev[0].isoformat()] if prev else [information_date]
        else:
            prev_ref = self._previous_period_reference(information_date, frequency)
            baseline_dates = self._period_dates(prev_ref, reading_mode, frequency)
        return current_dates, baseline_dates

    def _metrics_for_variable(self, var_type: str, data_type: str) -> List[str]:
        """Helper interno que realiza la operación "metrics_for_variable"."""
        metrics = ["null_rate"]
        if var_type in ("raw", "input", "transformed"):
            metrics.append("cardinality_ratio")
            if data_type == "numeric":
                metrics.extend(["outlier_rate", "psi_canonical", "psi_dynamic", "ks_vs_dev"])
            elif data_type == "categorical":
                metrics.extend([
                    "dominant_category_rate",
                    "category_composition_drift",
                    "psi_canonical",
                    "psi_dynamic",
                ])
        elif var_type == "score":
            metrics.extend([
                "range_violation",
                "null_rate",
                "entropy",
                "approval_rate",
                "psi_canonical",
                "psi_dynamic",
                "tail_shift",
                "concentration_gini",
                "psi_approved",
                "psi_rejected",
            ])
        elif var_type == "target":
            metrics.extend(["event_rate", "psi_target"])
        return metrics

    def _thresholds_for(
        self,
        variable: str,
        var_type: str,
        metric_name: str,
        thresholds_table: Dict,
        metric_threshold_auto: Dict,
        has_baseline: bool,
    ) -> Dict:
        """Helper interno que realiza la operación "thresholds_for"."""
        thresholds = dict(DEFAULT_THRESHOLDS.get(metric_name, {}))
        auto = metric_threshold_auto.get((variable, metric_name), {})
        for key in ("threshold_ambar", "threshold_red"):
            if auto.get(key) is not None:
                thresholds[key] = auto[key]
        if metric_name in ("psi_canonical", "psi_target"):
            t = thresholds_table.get((variable, var_type), {})
            if t.get("psi_threshold_ambar") is not None:
                thresholds["threshold_ambar"] = t["psi_threshold_ambar"]
            if t.get("psi_threshold_red") is not None:
                thresholds["threshold_red"] = t["psi_threshold_red"]
        if metric_name == "psi_dynamic":
            t = thresholds_table.get((variable, var_type), {})
            if t.get("psi_variation_threshold_ambar") is not None:
                thresholds["threshold_ambar"] = t["psi_variation_threshold_ambar"]
            if t.get("psi_variation_threshold_red") is not None:
                thresholds["threshold_red"] = t["psi_variation_threshold_red"]
        if not has_baseline and metric_name in RELATIVE_METRICS:
            thresholds = {k: v for k, v in thresholds.items() if k not in ("threshold_ambar", "threshold_red")}
        return thresholds

    def _join_conjugate(self, score_df: Any, target_df: Any, score_col: Any, target_col: Any) -> Any:
        """Helper interno que une conjugate."""
        join_cols = [c for c in self.join_keys if c in score_df.columns and c in target_df.columns]
        s = score_df.withColumnRenamed(score_col, "score")
        t = target_df.withColumnRenamed(target_col, "target")
        return s.join(t, on=join_cols, how="inner")

    def _checkpoint_key(
        self,
        model_id: str,
        information_date: str,
        baseline_date: Optional[str],
        model_summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Construye la clave determinística del checkpoint para una corrida."""
        return {
            "model_id": model_id,
            "information_date": information_date,
            "baseline_date": baseline_date or "auto",
            "frequency": str(model_summary.get("frequency", "daily")),
            "model_summary_process_date": str(model_summary.get("process_date", "")),
        }

    def _load_results_from_checkpoint(
        self,
        checkpoint_key: Dict[str, Any],
        execution_id: str,
    ) -> List[MetricResult]:
        """Recupera resultados de un checkpoint y actualiza el execution_id."""
        results_df = self.checkpoint.read(checkpoint_key, "results")
        results = []
        for row in results_df.collect():
            row_dict = row.asDict()
            row_dict["execution_id"] = execution_id
            results.append(MetricResult(**row_dict))
        return results

    def _load_summaries_from_checkpoint(
        self,
        checkpoint_key: Dict[str, Any],
        execution_id: str,
    ) -> List[Dict[str, Any]]:
        """Recupera resúmenes de un checkpoint y actualiza el execution_id."""
        summaries_df = self.checkpoint.read(checkpoint_key, "summaries")
        summaries = []
        for row in summaries_df.collect():
            row_dict = row.asDict()
            row_dict["execution_id"] = execution_id
            summaries.append(row_dict)
        return summaries

    @staticmethod
    def _results_schema() -> StructType:
        """Esquema para persistir MetricResult en parquet."""
        return StructType(
            [
                StructField("model_id", StringType(), True),
                StructField("information_date", StringType(), True),
                StructField("variable", StringType(), True),
                StructField("var_type", StringType(), True),
                StructField("metric_name", StringType(), True),
                StructField("metric_value", DoubleType(), True),
                StructField("baseline_value", DoubleType(), True),
                StructField("threshold_ambar", DoubleType(), True),
                StructField("threshold_red", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("baseline_process_date", StringType(), True),
                StructField("execution_id", StringType(), True),
                StructField("run_date", TimestampType(), True),
            ]
        )

    def _results_to_df(self, results: List[MetricResult]) -> Any:
        """Convierte una lista de MetricResult a DataFrame."""
        rows = [dataclasses.asdict(r) for r in results]
        return self.spark.createDataFrame(rows, schema=self._results_schema())

    @staticmethod
    def _summaries_schema() -> StructType:
        """Esquema para persistir resúmenes de variables en parquet."""
        return StructType(
            [
                StructField("execution_id", StringType(), True),
                StructField("variable", StringType(), True),
                StructField("var_type", StringType(), True),
                StructField("data_type", StringType(), True),
                StructField("model_id", StringType(), True),
                StructField("information_date", StringType(), True),
                StructField("statistic", StringType(), True),
                StructField("statistic_value", DoubleType(), True),
                StructField("statistic_value_str", StringType(), True),
            ]
        )

    def _summaries_to_df(self, summaries: List[Dict[str, Any]]) -> Any:
        """Convierte una lista de resúmenes a DataFrame."""
        return self.spark.createDataFrame(summaries, schema=self._summaries_schema())
