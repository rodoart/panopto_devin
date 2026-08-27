"""Módulo aggregator con la(s) clase(s) AggregateAlert, AlertAggregator."""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from mecv.metrics.result import MetricResult


DEFAULT_ALERT_POLICY = {
    "raw": {"red_equivalent": 3, "alert_ambar_pct": 0.60, "alert_red_pct": 0.40},
    "input": {"red_equivalent": 3, "alert_ambar_pct": 0.50, "alert_red_pct": 0.30},
    "transformed": {"red_equivalent": 3, "alert_ambar_pct": 0.50, "alert_red_pct": 0.30},
    "score": {"red_equivalent": 2, "alert_ambar_pct": 0.30, "alert_red_pct": 0.15},
    "target": {"red_equivalent": 3, "alert_ambar_pct": 0.50, "alert_red_pct": 0.30},
    "conjugate": {"red_equivalent": 2, "alert_ambar_pct": 0.50, "alert_red_pct": 0.30},
}


@dataclass
class AggregateAlert:
    """Clase de datos que representa AggregateAlert."""
    model_id: str
    information_date: str
    var_type: str
    total_metrics: int
    count_ambar: int
    count_red: int
    equivalent_yellow: float
    stress_ratio: float
    aggregate_status: str
    red_equivalent: int
    alert_ambar_pct: float
    alert_red_pct: float
    alert_sent: bool = False
    alert_type: Optional[str] = None


class AlertAggregator:
    """Clase que representa AlertAggregator."""
    def __init__(self, policy: Optional[Dict[str, Dict]] = None) -> None:
        """Inicializa una nueva instancia de AlertAggregator."""
        self.policy = policy or DEFAULT_ALERT_POLICY

    def aggregate(self, results: List[MetricResult]) -> List[AggregateAlert]:
        """Método que agrupa."""
        groups = defaultdict(list)
        for r in results:
            key = (r.model_id, r.information_date, r.var_type)
            groups[key].append(r)
        alerts = []
        for (model_id, information_date, var_type), metrics in groups.items():
            policy = self.policy.get(var_type, {})
            red_eq = policy.get("red_equivalent", 3)
            ambar_pct = policy.get("alert_ambar_pct", 0.50)
            red_pct = policy.get("alert_red_pct", 0.30)
            total = len(metrics)
            ambar = sum(1 for m in metrics if m.status == "AMBAR")
            red = sum(1 for m in metrics if m.status == "RED")
            equivalent_yellow = ambar + red * red_eq
            stress = equivalent_yellow / (total * red_eq) if total else 1.0
            if stress >= red_pct:
                status = "RED"
            elif stress >= ambar_pct:
                status = "AMBAR"
            else:
                status = "GREEN"
            alerts.append(
                AggregateAlert(
                    model_id=model_id,
                    information_date=information_date,
                    var_type=var_type,
                    total_metrics=total,
                    count_ambar=ambar,
                    count_red=red,
                    equivalent_yellow=float(equivalent_yellow),
                    stress_ratio=float(stress),
                    aggregate_status=status,
                    red_equivalent=red_eq,
                    alert_ambar_pct=ambar_pct,
                    alert_red_pct=red_pct,
                )
            )
        return alerts
