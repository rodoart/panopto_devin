from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class MetricResult:
    model_id: str
    information_date: str
    variable: str
    var_type: str
    metric_name: str
    metric_value: float
    baseline_value: Optional[float]
    threshold_ambar: Optional[float]
    threshold_red: Optional[float]
    status: str
    baseline_process_date: Optional[str] = None
    execution_id: Optional[str] = None
    run_date: Optional[datetime] = None
