"""Módulo summary con la(s) clase(s) VariableSummaryBuilder, ScoringSummaryBuilder."""

from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from panopto.metrics.result import MetricResult


class VariableSummaryBuilder:
    """Clase que representa VariableSummaryBuilder."""
    @staticmethod
    def build(
        df: DataFrame,
        variable: str,
        var_type: str,
        data_type: str,
        model_id: str,
        information_date: str,
        execution_id: str,
    ) -> List[Dict[str, Any]]:
        """Método estático que construye."""
        rows = []
        base = {
            "execution_id": execution_id,
            "variable": variable,
            "var_type": var_type,
            "data_type": data_type,
            "model_id": model_id,
            "information_date": information_date,
        }

        count_row = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.count(F.col(variable)).alias("non_null"),
        ).collect()[0]
        total = count_row["total"] or 0
        non_null = count_row["non_null"] or 0
        nulls = total - non_null

        rows.append({**base, "statistic": "count_total", "statistic_value": float(total), "statistic_value_str": str(total)})
        rows.append({**base, "statistic": "count_non_null", "statistic_value": float(non_null), "statistic_value_str": str(non_null)})
        rows.append({**base, "statistic": "count_null", "statistic_value": float(nulls), "statistic_value_str": str(nulls)})

        if data_type == "numeric" and non_null > 0:
            agg_row = df.agg(
                F.min(F.col(variable)).alias("min"),
                F.max(F.col(variable)).alias("max"),
                F.mean(F.col(variable)).alias("mean"),
                F.stddev_samp(F.col(variable)).alias("std"),
            ).collect()[0]
            for stat in ("min", "max", "mean", "std"):
                val = agg_row[stat]
                rows.append({**base, "statistic": stat, "statistic_value": float(val if val is not None else 0.0), "statistic_value_str": str(val)})

            for q in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
                val = df.approxQuantile(variable, [q], 0.01)[0]
                rows.append({
                    **base,
                    "statistic": f"p{int(q * 100)}",
                    "statistic_value": float(val),
                    "statistic_value_str": str(val),
                })
        elif data_type == "categorical":
            distinct = df.filter(F.col(variable).isNotNull()).select(F.col(variable)).distinct().count()
            rows.append({**base, "statistic": "distinct_count", "statistic_value": float(distinct), "statistic_value_str": str(distinct)})

            top = df.groupBy(F.col(variable).alias("category")).count().orderBy(F.desc("count")).limit(1).collect()
            if top:
                rows.append({**base, "statistic": "top_category", "statistic_value": None, "statistic_value_str": str(top[0]["category"])})
                rows.append({**base, "statistic": "top_category_count", "statistic_value": float(top[0]["count"]), "statistic_value_str": str(top[0]["count"])})

        return rows


# Nombres de las métricas que alimentan el control oficial "Pre Scoring":
# %Raw variables above median variation thresholds, Observation windows
# availability y %Raw sources with growth rt within Thresholds.
PRE_SCORING_METRIC_NAMES = {"median_shift", "population_variation", "completeness"}


class ScoringSummaryBuilder:
    """Construye la fila canónica "Summary Scoring Monitoring".

    No calcula ninguna métrica nueva: agrega los mismos ``MetricResult``
    producidos por :class:`panopto.metrics.runner.MetricRunner` (mismo motor)
    para reproducir las columnas oficiales Model / Scoring Dt / Vintage /
    Control Data Availability / Control Pre Scoring / PSI / PSI Variation /
    CSI Max / CSI / Population Scored / General Status.
    """

    @staticmethod
    def build(
        model_id: str,
        model_name: str,
        information_date: str,
        vintage: str,
        results: List[MetricResult],
        summaries: List[Dict[str, Any]],
        data_availability_done: bool,
    ) -> Dict[str, Any]:
        """Método estático que construye la fila canónica."""

        def _first(var_type: str, metric_name: str) -> Optional[MetricResult]:
            for r in results:
                if r.var_type == var_type and r.metric_name == metric_name:
                    return r
            return None

        # PSI (contra bins canónicos de dev) y PSI Variation (contra el
        # periodo baseline inmediato) del score, ya calculados por el motor.
        psi_result = _first("score", "psi_canonical")
        psi_variation_result = _first("score", "psi_dynamic")

        # CSI: mismo psi_canonical, pero aplicado a las variables raw/input.
        csi_results = [
            r for r in results if r.var_type in ("raw", "input") and r.metric_name == "psi_canonical"
        ]
        csi_max = max((r.metric_value for r in csi_results), default=None)
        csi_status = "Stop" if any(r.status == "RED" for r in csi_results) else "OK"

        # Population Scored: conteo real de la población scoreada (var_type=score).
        population_scored = next(
            (
                s.get("statistic_value")
                for s in summaries
                if s.get("var_type") == "score" and s.get("statistic") == "count_total"
            ),
            None,
        )

        # Control Pre Scoring: OK salvo que alguno de los 3 chequeos oficiales rompa su umbral.
        prescoring_results = [
            r for r in results if r.var_type in ("raw", "input") and r.metric_name in PRE_SCORING_METRIC_NAMES
        ]
        control_pre_scoring = "Reprocess" if any(r.status == "RED" for r in prescoring_results) else "OK"

        worst = "OK"
        for r in results:
            if r.status == "RED":
                worst = "RED"
                break
            if r.status == "AMBAR":
                worst = "AMBAR"

        if not data_availability_done:
            general_status = "DQR Process Pending"
        elif control_pre_scoring == "Reprocess":
            general_status = "Reprocess"
        elif worst in ("RED", "AMBAR") or csi_status == "Stop":
            general_status = "WARNING"
        else:
            general_status = "OK"

        return {
            "model_id": model_id,
            "model_name": model_name,
            "information_date": information_date,
            "scoring_date": information_date,
            "vintage": vintage,
            "control_data_availability": "DONE" if data_availability_done else "PENDING",
            "control_pre_scoring": control_pre_scoring,
            "psi": psi_result.metric_value if psi_result else None,
            "psi_variation": psi_variation_result.metric_value if psi_variation_result else None,
            "csi_max": csi_max,
            "csi_status": csi_status,
            "population_scored": population_scored,
            "general_status": general_status,
        }
