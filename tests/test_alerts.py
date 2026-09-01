"""Tests for alert aggregation and email dispatch."""

import json

from mecv.alerts.aggregator import AggregateAlert, AlertAggregator
from mecv.alerts.dispatcher import EmailDispatcher
from mecv.alerts.email_builder import EmailBuilder
from mecv.metrics.result import MetricResult


def test_alert_aggregator_counts_statuses():
    """AlertAggregator groups MetricResult objects and computes aggregate status."""
    results = [
        MetricResult(
            model_id="M1",
            information_date="2025-01-01",
            variable="age",
            var_type="raw",
            metric_name="null_rate",
            metric_value=0.05,
            baseline_value=0.02,
            threshold_ambar=0.05,
            threshold_red=0.10,
            status="AMBAR",
        ),
        MetricResult(
            model_id="M1",
            information_date="2025-01-01",
            variable="age",
            var_type="raw",
            metric_name="outlier_rate",
            metric_value=0.12,
            baseline_value=0.01,
            threshold_ambar=0.03,
            threshold_red=0.06,
            status="RED",
        ),
        MetricResult(
            model_id="M1",
            information_date="2025-01-01",
            variable="score",
            var_type="score",
            metric_name="range_violation",
            metric_value=0.0,
            baseline_value=None,
            threshold_ambar=None,
            threshold_red=1e-9,
            status="GREEN",
        ),
    ]

    aggregator = AlertAggregator()
    alerts = aggregator.aggregate(results)

    assert len(alerts) == 2
    raw_alert = next(a for a in alerts if a.var_type == "raw")
    score_alert = next(a for a in alerts if a.var_type == "score")

    assert raw_alert.total_metrics == 2
    assert raw_alert.count_red == 1
    assert raw_alert.count_ambar == 1
    # raw policy: red_equivalent=3, ambar_pct=0.6, red_pct=0.4
    # stress = (1 + 1*3) / (2*3) = 0.667 -> RED
    assert raw_alert.aggregate_status == "RED"

    assert score_alert.total_metrics == 1
    assert score_alert.count_red == 0
    assert score_alert.aggregate_status == "GREEN"


def test_email_dispatcher_build_recipients_red_and_missing(tmp_path, postgres_connection):
    """_build_recipients selects contacts based on alert/missing flags."""
    config_path = tmp_path / "email_config.json"
    config_path.write_text(
        json.dumps({
            "sender_name": "MECV",
            "sender_email": "alerts@example.com",
            "subject_prefix": "[MECV]",
        })
    )

    class AlertStub:
        aggregate_status = "RED"

    fake = postgres_connection
    fake.set_results([
        ("owner@example.com", True, True, True),
        ("red-contact@example.com", False, True, False),
    ])
    fake.add_query_results("is_active = true", [("red-list@example.com",)])

    dispatcher = EmailDispatcher(config_path=str(config_path))
    to, bcc = dispatcher._build_recipients("M1", [AlertStub()], missing_data=False)

    assert "owner@example.com" in to
    assert "red-contact@example.com" in to
    assert "red-list@example.com" in bcc

    # Missing data case
    fake.set_results([("owner@example.com", True, True, True)])
    to, bcc = dispatcher._build_recipients("M1", [], missing_data=True)
    assert "owner@example.com" in to


def test_email_builder_builds_html():
    """EmailBuilder produces a valid HTML body containing model metadata."""
    config = {
        "sender_name": "MECV",
        "sender_email": "alerts@example.com",
        "subject_prefix": "[MECV]",
    }
    builder = EmailBuilder(config)
    html = builder.build_html(
        model_id="M1",
        model_name="Model One",
        information_date="2025-01-01",
        aggregate_alerts=[],
        metric_results=[],
    )
    assert "<html" in html
    assert "M1" in html
    assert "2025-01-01" in html
