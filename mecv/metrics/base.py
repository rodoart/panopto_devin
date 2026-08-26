from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional, Type

from pyspark.sql import DataFrame
from mecv.metrics.result import MetricResult


class Metric(ABC):
    name = "metric"

    @abstractmethod
    def calculate(
        self,
        df: DataFrame,
        baseline: Optional[DataFrame],
        thresholds: Dict[str, float],
        **params,
    ) -> MetricResult:
        raise NotImplementedError

    def _make_result(
        self,
        metric_value: float,
        baseline_value: Optional[float],
        thresholds: Dict[str, float],
        **params,
    ) -> MetricResult:
        ambar = thresholds.get("threshold_ambar")
        red = thresholds.get("threshold_red")
        value = 0.0 if metric_value is None else float(metric_value)
        if red is not None and value >= red:
            status = "RED"
        elif ambar is not None and value >= ambar:
            status = "AMBAR"
        elif ambar is not None or red is not None:
            status = "GREEN"
        else:
            status = "NOT_APPLICABLE"
        return MetricResult(
            model_id=params.get("model_id", ""),
            information_date=params.get("information_date", ""),
            variable=params.get("variable", ""),
            var_type=params.get("var_type", ""),
            metric_name=self.name,
            metric_value=value,
            baseline_value=baseline_value,
            threshold_ambar=ambar,
            threshold_red=red,
            status=status,
            baseline_process_date=params.get("baseline_process_date"),
            execution_id=params.get("execution_id"),
            run_date=datetime.now(),
        )


class MetricRegistry:
    _registry: Dict[str, Type[Metric]] = {}

    @classmethod
    def register(cls, metric_class: Type[Metric]):
        cls._registry[metric_class.name] = metric_class

    @classmethod
    def get(cls, name: str) -> Type[Metric]:
        if name not in cls._registry:
            raise KeyError(f"metric not registered: {name}")
        return cls._registry[name]

    @classmethod
    def list(cls):
        return sorted(cls._registry.keys())
