"""Módulo dispatcher con la(s) clase(s) EmailLog, EmailDispatcher."""

import json
import os
import smtplib
import uuid
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any, Dict, List, Optional, Tuple

from mecv.alerts.aggregator import AggregateAlert
from mecv.alerts.email_builder import EmailBuilder
from mecv.config import Settings
from mecv.logging import get_logger
from mecv.metrics.result import MetricResult
from mecv.sessions import PostgresSession

logger = get_logger(__name__)


@dataclass
class EmailLog:
    """Clase de datos que representa EmailLog."""
    email_id: str
    execution_id: Optional[str]
    alert_type: str
    recipients_to: str
    recipients_bcc: str
    subject: str
    body_summary: str
    sent_timestamp: datetime
    status: str
    smtp_response: str
    retry_count: int = 0


class EmailDispatcher:
    """Clase que representa EmailDispatcher."""
    def __init__(self, config_path: Optional[str] = None) -> None:
        """Inicializa una nueva instancia de EmailDispatcher."""
        self.config = self._load_config(config_path)
        self.settings = Settings.from_env()
        self.psql = PostgresSession()

    @staticmethod
    def _load_config(config_path: Optional[str]) -> Dict[str, Any]:
        """Helper interno que carga config."""
        path = config_path or os.environ.get("MECV_EMAIL_CONFIG_PATH", "config/email_config.json")
        with open(path, "r") as f:
            return json.load(f)

    def dispatch(
        self,
        model_id: str,
        information_date: str,
        aggregate_alerts: List[AggregateAlert],
        metric_results: List[MetricResult],
        model_name: str = "",
        missing_data: bool = False,
        missing_days: int = 0,
        execution_id: Optional[str] = None,
    ) -> EmailLog:
        """Método que envía."""
        model_id = str(model_id)
        information_date = str(information_date)
        alert_type = "MISSING_DATA" if missing_data else self._overall_status(aggregate_alerts)
        logger.info(f"dispatching {alert_type} for {model_id} {information_date}")
        to_list, bcc_list = self._build_recipients(model_id, aggregate_alerts, missing_data)
        subject = self._build_subject(model_id, information_date, alert_type)
        html = EmailBuilder(self.config).build_html(
            model_id=model_id,
            model_name=model_name,
            information_date=information_date,
            aggregate_alerts=aggregate_alerts,
            metric_results=metric_results,
            missing_data=missing_data,
            missing_days=missing_days,
        )
        status, smtp_response = self._send_email(to_list, bcc_list, subject, html)
        return EmailLog(
            email_id=str(uuid.uuid4()),
            execution_id=execution_id,
            alert_type=alert_type,
            recipients_to=", ".join(to_list),
            recipients_bcc=", ".join(bcc_list),
            subject=subject,
            body_summary=html[:500],
            sent_timestamp=datetime.now(),
            status=status,
            smtp_response=smtp_response,
        )

    def _build_recipients(self, model_id: str, aggregate_alerts: List[AggregateAlert], missing_data: bool) -> Tuple[Any, ...]:
        """Helper interno que construye recipients."""
        has_red = any(a.aggregate_status == "RED" for a in aggregate_alerts)
        has_ambar = any(a.aggregate_status == "AMBAR" for a in aggregate_alerts)
        to_set = set()
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT contact_email, notify_on_ambar, notify_on_red, notify_on_missing
                    FROM model_contact_d_t_d
                    WHERE model_id = %s
                      AND process_date = (
                          SELECT max(process_date) FROM model_contact_d_t_d WHERE model_id = %s
                      )
                """,
                    (model_id, model_id),
                )
                for email, on_ambar, on_red, on_missing in cur.fetchall():
                    if missing_data and on_missing:
                        to_set.add(email)
                    if has_red and on_red:
                        to_set.add(email)
                    if has_ambar and on_ambar:
                        to_set.add(email)
        bcc_set = set()
        if has_red or missing_data:
            with self.psql.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT email FROM red_alert_list_d WHERE is_active = true")
                    for row in cur.fetchall():
                        bcc_set.add(row[0])
        to_list = sorted(to_set)
        bcc_list = sorted(bcc_set - to_set)
        if not to_list and bcc_list:
            to_list = bcc_list
            bcc_list = []
        return to_list, bcc_list

    def _overall_status(self, aggregate_alerts: List[AggregateAlert]) -> str:
        """Helper interno que realiza la operación "overall_status"."""
        statuses = {a.aggregate_status for a in aggregate_alerts}
        if "RED" in statuses:
            return "RED"
        if "AMBAR" in statuses:
            return "AMBAR"
        return "GREEN"

    def _build_subject(self, model_id: str, information_date: str, alert_type: str) -> str:
        """Helper interno que construye subject."""
        prefix = self.config.get("subject_prefix", "[MECV]")
        return f"{prefix} {alert_type} - {model_id} - {information_date}"

    def _send_email(self, to_list: List[str], bcc_list: List[str], subject: str, html: str) -> Tuple[Any, ...]:
        """Helper interno que envía email."""
        sender_email = self.config.get("sender_email", "")
        sender_name = self.config.get("sender_name", "")
        if not sender_email:
            return "FAILED", "sender_email not configured in email config"
        if not to_list:
            return "SKIPPED", "no recipients"
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = formataddr((sender_name, sender_email))
        msg["To"] = ", ".join(to_list)
        if bcc_list:
            msg["Bcc"] = ", ".join(bcc_list)
        msg.attach(MIMEText(html, "html"))
        all_recipients = to_list + bcc_list
        try:
            use_ssl = self.settings.smtp_port == 465 or os.getenv("MECV_SMTP_USE_SSL", "false").lower() == "true"
            if use_ssl:
                server = smtplib.SMTP_SSL(self.settings.smtp_host, self.settings.smtp_port)
            else:
                server = smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port)
                if self.settings.smtp_port == 587 or os.getenv("MECV_SMTP_USE_TLS", "true").lower() == "true":
                    server.starttls()
            server.login(self.settings.smtp_user, self.settings.smtp_password)
            response = server.sendmail(sender_email, all_recipients, msg.as_string())
            server.quit()
            return "SENT", str(response) if response else "OK"
        except Exception as exc:
            return "FAILED", str(exc)
