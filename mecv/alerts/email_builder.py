import json
import os
from html import escape
from string import Template
from typing import Any, Dict, List

from mecv.alerts.aggregator import AggregateAlert
from mecv.metrics.result import MetricResult


BASE_TEMPLATE = Template("""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>$title</title>
<style>
body { font-family: Arial, Helvetica, sans-serif; color: #333; line-height: 1.4; }
.container { max-width: 900px; margin: 0 auto; padding: 20px; }
h1 { color: #1a1a1a; }
.status-green { color: #2e7d32; font-weight: bold; }
.status-ambar { color: #f9a825; font-weight: bold; }
.status-red { color: #c62828; font-weight: bold; }
.status-na { color: #757575; font-weight: bold; }
table { border-collapse: collapse; width: 100%; margin: 16px 0; }
th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
th { background-color: #f5f5f5; }
.banner { padding: 12px; border-radius: 4px; margin: 16px 0; }
.banner-red { background-color: #ffebee; border-left: 4px solid #c62828; }
.section { margin: 24px 0; }
</style>
</head>
<body>
<div class="container">
$body
</div>
</body>
</html>""")


class EmailBuilder:
    def __init__(self, config: Dict[str, Any] = None):
        if config is None:
            config = self._default_config()
        self.config = config

    @staticmethod
    def _default_config() -> Dict[str, Any]:
        path = os.environ.get("MECV_EMAIL_CONFIG_PATH", "config/email_config.json")
        with open(path, "r") as f:
            return json.load(f)

    def build_html(
        self,
        model_id: str,
        model_name: str,
        information_date: str,
        aggregate_alerts: List[AggregateAlert],
        metric_results: List[MetricResult],
        missing_data: bool = False,
        missing_days: int = 0,
    ) -> str:
        title = f"{self.config.get('subject_prefix', '[MECV]')} Alerta {model_id} - {information_date}"
        body = self._build_body(
            model_id,
            model_name,
            information_date,
            aggregate_alerts,
            metric_results,
            missing_data,
            missing_days,
        )
        return BASE_TEMPLATE.safe_substitute(title=escape(title), body=body)

    def _build_body(
        self,
        model_id: str,
        model_name: str,
        information_date: str,
        aggregate_alerts: List[AggregateAlert],
        metric_results: List[MetricResult],
        missing_data: bool,
        missing_days: int,
    ) -> str:
        parts = [
            f"<h1>Alerta MECV: {escape(model_name)} ({escape(model_id)})</h1>",
            f"<p><strong>Fecha de información:</strong> {escape(information_date)}</p>",
        ]
        if missing_data:
            parts.append(
                f'<div class="banner banner-red"><strong>Datos faltantes:</strong> '
                f'El modelo no recibió datos para information_date = {escape(information_date)}. '
                f'Días consecutivos sin datos: {missing_days}.</div>'
            )
        parts.append(self._summary_table(aggregate_alerts))
        parts.append(self._red_metrics_section(metric_results))
        parts.append(self._conjugate_section(metric_results))
        parts.append(self._target_section(metric_results))
        sender = self.config.get("sender_email", "")
        name = self.config.get("sender_name", "")
        parts.append(
            f'<div class="section"><p style="font-size: 12px; color: #757575;">'
            f'Enviado por {escape(name)} &lt;{escape(sender)}&gt;</p></div>'
        )
        return "\n".join(parts)

    def _summary_table(self, aggregate_alerts: List[AggregateAlert]) -> str:
        rows = []
        rows.append(
            "<tr><th>Tipo de variable</th><th>Estado</th><th>Total métricas</th>"
            "<th>Ámbar</th><th>Rojo</th><th>Stress ratio</th></tr>"
        )
        for a in aggregate_alerts:
            rows.append(
                f"<tr><td>{escape(a.var_type)}</td>"
                f'<td class="{self._status_class(a.aggregate_status)}">{escape(a.aggregate_status)}</td>'
                f"<td>{a.total_metrics}</td><td>{a.count_ambar}</td><td>{a.count_red}</td>"
                f"<td>{a.stress_ratio:.3f}</td></tr>"
            )
        return '<div class="section"><h2>Resumen por tipo de variable</h2><table>{}</table></div>'.format(
            "".join(rows)
        )

    def _red_metrics_section(self, metric_results: List[MetricResult]) -> str:
        red = [m for m in metric_results if m.status == "RED"]
        if not red:
            return ""
        rows = ["<tr><th>Variable</th><th>Métrica</th><th>Valor</th><th>Umbral rojo</th></tr>"]
        for m in red:
            rows.append(
                f"<tr><td>{escape(m.variable)}</td><td>{escape(m.metric_name)}</td>"
                f"<td>{m.metric_value:.6f}</td><td>{m.threshold_red}</td></tr>"
            )
        return '<div class="section"><h2 class="status-red">Métricas en rojo</h2><table>{}</table></div>'.format(
            "".join(rows)
        )

    def _conjugate_section(self, metric_results: List[MetricResult]) -> str:
        names = {"auc", "gini", "brier_score", "lift_top_decile"}
        metrics = [m for m in metric_results if m.metric_name in names]
        if not metrics:
            return ""
        rows = ["<tr><th>Métrica</th><th>Valor</th><th>Baseline</th><th>Estado</th></tr>"]
        for m in metrics:
            rows.append(
                f"<tr><td>{escape(m.metric_name)}</td><td>{m.metric_value:.6f}</td>"
                f"<td>{m.baseline_value}</td><td class=\"{self._status_class(m.status)}\">{escape(m.status)}</td></tr>"
            )
        return '<div class="section"><h2>Desempeño del modelo (Score + Target)</h2><table>{}</table></div>'.format(
            "".join(rows)
        )

    def _target_section(self, metric_results: List[MetricResult]) -> str:
        metrics = [m for m in metric_results if m.var_type == "target"]
        if not metrics:
            return ""
        rows = ["<tr><th>Métrica</th><th>Valor</th><th>Baseline</th><th>Estado</th></tr>"]
        for m in metrics:
            rows.append(
                f"<tr><td>{escape(m.metric_name)}</td><td>{m.metric_value:.6f}</td>"
                f"<td>{m.baseline_value}</td><td class=\"{self._status_class(m.status)}\">{escape(m.status)}</td></tr>"
            )
        return '<div class="section"><h2>Target</h2><table>{}</table></div>'.format("".join(rows))

    @staticmethod
    def _status_class(status: str) -> str:
        return {
            "GREEN": "status-green",
            "AMBAR": "status-ambar",
            "RED": "status-red",
        }.get(status, "status-na")
