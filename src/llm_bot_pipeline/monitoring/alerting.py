"""
Alerting framework for pipeline monitoring.

Provides:
- Email notifications
- Webhook notifications (Slack, Discord, etc.)
- Alert deduplication
- Escalation policies
"""

import hashlib
import html
import json
import logging
import smtplib
import threading
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents an alert to be sent."""

    title: str
    message: str
    severity: AlertSeverity
    source: str  # e.g., "pipeline", "data_quality"
    alert_type: str  # e.g., "pipeline_failure", "freshness_check"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    @property
    def fingerprint(self) -> str:
        """Generate unique fingerprint for deduplication."""
        content = f"{self.source}:{self.alert_type}:{self.severity.value}:{self.title}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "source": self.source,
            "alert_type": self.alert_type,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "tags": self.tags,
            "fingerprint": self.fingerprint,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class AlertNotifier(ABC):
    """Abstract base class for alert notifiers."""

    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """
        Send an alert notification.

        Args:
            alert: Alert to send

        Returns:
            True if notification was sent successfully
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test the notification connection.

        Returns:
            True if connection is working
        """
        pass


class EmailNotifier(AlertNotifier):
    """Send alerts via email using SMTP."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        smtp_user: str,
        smtp_password: str,
        from_address: str,
        to_addresses: list[str],
        use_tls: bool = True,
    ):
        """
        Initialize email notifier.

        Args:
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            smtp_user: SMTP username
            smtp_password: SMTP password
            from_address: Sender email address
            to_addresses: List of recipient email addresses
            use_tls: Whether to use TLS
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_address = from_address
        self.to_addresses = to_addresses
        self.use_tls = use_tls

    def send(self, alert: Alert) -> bool:
        """Send alert via email."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{alert.severity.value.upper()}] {alert.title}"
            msg["From"] = self.from_address
            msg["To"] = ", ".join(self.to_addresses)

            text_body = self._format_text(alert)
            msg.attach(MIMEText(text_body, "plain"))

            html_body = self._format_html(alert)
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_address, self.to_addresses, msg.as_string())

            logger.info(f"Email alert sent: {alert.title}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def test_connection(self) -> bool:
        """Test SMTP connection."""
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_password)
            return True
        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}")
            return False

    def _format_text(self, alert: Alert) -> str:
        """Format alert as plain text."""
        lines = [
            f"Alert: {alert.title}",
            f"Severity: {alert.severity.value}",
            f"Source: {alert.source}",
            f"Time: {alert.timestamp.isoformat()}",
            "",
            alert.message,
        ]

        if alert.details:
            lines.append("")
            lines.append("Details:")
            for key, value in alert.details.items():
                lines.append(f"  {key}: {value}")

        return "\n".join(lines)

    def _format_html(self, alert: Alert) -> str:
        """Format alert as HTML."""
        severity_colors = {
            AlertSeverity.INFO: "#3498db",
            AlertSeverity.WARNING: "#f39c12",
            AlertSeverity.ERROR: "#e74c3c",
            AlertSeverity.CRITICAL: "#8e44ad",
        }
        color = severity_colors.get(alert.severity, "#333")

        title = html.escape(alert.title)
        message = html.escape(alert.message)
        source = html.escape(alert.source)
        severity = html.escape(alert.severity.value)

        details_html = ""
        if alert.details:
            details_rows = "".join(
                f"<tr><td><strong>{html.escape(str(k))}</strong></td>"
                f"<td>{html.escape(str(v))}</td></tr>"
                for k, v in alert.details.items()
            )
            details_html = f"""
            <h3>Details</h3>
            <table border="1" cellpadding="5" cellspacing="0">
                {details_rows}
            </table>
            """

        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <div style="border-left: 4px solid {color}; padding-left: 15px;">
                <h2 style="color: {color}; margin-top: 0;">
                    [{severity.upper()}] {title}
                </h2>
                <p><strong>Source:</strong> {source}</p>
                <p><strong>Time:</strong> {alert.timestamp.isoformat()}</p>
                <hr>
                <p>{message}</p>
                {details_html}
            </div>
        </body>
        </html>
        """


class WebhookNotifier(AlertNotifier):
    """Send alerts via webhook (Slack, Discord, generic)."""

    def __init__(
        self,
        webhook_url: str,
        webhook_type: str = "generic",
        custom_headers: Optional[dict] = None,
        timeout_seconds: int = 10,
    ):
        """
        Initialize webhook notifier.

        Args:
            webhook_url: Webhook endpoint URL
            webhook_type: Type of webhook (slack, discord, generic)
            custom_headers: Additional HTTP headers
            timeout_seconds: Request timeout
        """
        parsed = urlparse(webhook_url)
        if parsed.scheme != "https":
            raise ValueError(f"Webhook URL must use HTTPS, got: {parsed.scheme}")
        self.webhook_url = webhook_url
        self.webhook_type = webhook_type.lower()
        self.custom_headers = custom_headers or {}
        self.timeout_seconds = timeout_seconds

    def send(self, alert: Alert) -> bool:
        """Send alert via webhook."""
        try:
            payload = self._format_payload(alert)
            data = json.dumps(payload).encode("utf-8")

            headers = {
                "Content-Type": "application/json",
                **self.custom_headers,
            }

            request = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers=headers,
                method="POST",
            )

            with urllib.request.urlopen(
                request, timeout=self.timeout_seconds
            ) as response:
                if response.status in (200, 201, 204):
                    logger.info(f"Webhook alert sent: {alert.title}")
                    return True
                else:
                    logger.warning(f"Webhook returned status {response.status}")
                    return False

        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
            return False

    def test_connection(self) -> bool:
        """Test webhook connection with a test message."""
        test_alert = Alert(
            title="Test Alert",
            message="This is a test alert to verify webhook connectivity.",
            severity=AlertSeverity.INFO,
            source="alerting_test",
            alert_type="connectivity_test",
        )
        return self.send(test_alert)

    def _format_payload(self, alert: Alert) -> dict:
        """Format alert payload based on webhook type."""
        if self.webhook_type == "slack":
            return self._format_slack(alert)
        elif self.webhook_type == "discord":
            return self._format_discord(alert)
        else:
            return self._format_generic(alert)

    def _format_slack(self, alert: Alert) -> dict:
        """Format alert for Slack webhook."""
        severity_emoji = {
            AlertSeverity.INFO: ":information_source:",
            AlertSeverity.WARNING: ":warning:",
            AlertSeverity.ERROR: ":x:",
            AlertSeverity.CRITICAL: ":rotating_light:",
        }
        emoji = severity_emoji.get(alert.severity, ":bell:")

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {alert.title}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": alert.message,
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Severity:* {alert.severity.value} | "
                        f"*Source:* {alert.source} | "
                        f"*Time:* {alert.timestamp.isoformat()}",
                    }
                ],
            },
        ]

        if alert.details:
            details_text = "\n".join(f"• *{k}:* {v}" for k, v in alert.details.items())
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Details:*\n{details_text}",
                    },
                }
            )

        return {"blocks": blocks}

    def _format_discord(self, alert: Alert) -> dict:
        """Format alert for Discord webhook."""
        severity_colors = {
            AlertSeverity.INFO: 3447003,  # Blue
            AlertSeverity.WARNING: 16776960,  # Yellow
            AlertSeverity.ERROR: 15158332,  # Red
            AlertSeverity.CRITICAL: 9109504,  # Purple
        }
        color = severity_colors.get(alert.severity, 0)

        embed = {
            "title": alert.title,
            "description": alert.message,
            "color": color,
            "timestamp": alert.timestamp.isoformat(),
            "footer": {"text": f"Source: {alert.source}"},
        }

        if alert.details:
            embed["fields"] = [
                {"name": k, "value": str(v), "inline": True}
                for k, v in alert.details.items()
            ]

        return {"embeds": [embed]}

    def _format_generic(self, alert: Alert) -> dict:
        """Format alert as generic JSON."""
        return alert.to_dict()


class LogNotifier(AlertNotifier):
    """Log alerts to the Python logger (useful for testing/development)."""

    def __init__(self, logger_name: str = "alerts"):
        """
        Initialize log notifier.

        Args:
            logger_name: Name of the logger to use
        """
        self.alert_logger = logging.getLogger(logger_name)

    def send(self, alert: Alert) -> bool:
        """Log alert at appropriate level."""
        level_map = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.ERROR: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL,
        }
        level = level_map.get(alert.severity, logging.INFO)
        self.alert_logger.log(level, alert.to_json())
        return True

    def test_connection(self) -> bool:
        """Always returns True for log notifier."""
        return True


@dataclass
class AlertHistory:
    """Track alert history for deduplication."""

    fingerprint: str
    first_seen: datetime
    last_seen: datetime
    count: int = 1


class AlertManager:
    """
    Manages alert routing, deduplication, and escalation.

    Features:
    - Multiple notifiers
    - Alert deduplication within time window
    - Severity-based routing
    - Rate limiting
    """

    def __init__(
        self,
        dedupe_window_minutes: int = 60,
        max_alerts_per_hour: int = 20,
    ):
        """
        Initialize alert manager.

        Args:
            dedupe_window_minutes: Window for deduplication
            max_alerts_per_hour: Maximum alerts per hour (rate limiting)
        """
        self.dedupe_window = timedelta(minutes=dedupe_window_minutes)
        self.max_alerts_per_hour = max_alerts_per_hour
        self._lock = threading.Lock()

        self._notifiers: list[tuple[AlertNotifier, Optional[AlertSeverity]]] = []
        self._alert_history: dict[str, AlertHistory] = {}
        self._hourly_count = 0
        self._hour_start = datetime.now(timezone.utc)

    def add_notifier(
        self,
        notifier: AlertNotifier,
        min_severity: Optional[AlertSeverity] = None,
    ) -> None:
        """
        Add a notifier with optional severity filter.

        Args:
            notifier: Notifier instance
            min_severity: Minimum severity to send (None = all)
        """
        self._notifiers.append((notifier, min_severity))
        logger.info(f"Added notifier: {type(notifier).__name__}")

    def send_alert(
        self,
        alert: Alert,
        force: bool = False,
    ) -> bool:
        """
        Send an alert through all configured notifiers.

        Args:
            alert: Alert to send
            force: Force send even if deduplicated or rate limited

        Returns:
            True if alert was sent to at least one notifier
        """
        with self._lock:
            if not force and self._is_duplicate(alert):
                logger.debug(f"Alert deduplicated: {alert.fingerprint}")
                return False
            if not force and not self._check_rate_limit():
                logger.warning("Alert rate limit exceeded, skipping")
                return False

        any_sent = False
        for notifier, min_severity in self._notifiers:
            if self._should_notify(alert, min_severity):
                try:
                    if notifier.send(alert):
                        any_sent = True
                except Exception as e:
                    logger.error(f"Notifier {type(notifier).__name__} failed: {e}")

        with self._lock:
            self._hourly_count += 1
            self._track_alert(alert)

        return any_sent

    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits."""
        now = datetime.now(timezone.utc)

        if (now - self._hour_start).total_seconds() >= 3600:
            self._hourly_count = 0
            self._hour_start = now

        return self._hourly_count < self.max_alerts_per_hour

    def _is_duplicate(self, alert: Alert) -> bool:
        """Check if alert is a duplicate within dedupe window."""
        fingerprint = alert.fingerprint
        now = datetime.now(timezone.utc)

        if fingerprint in self._alert_history:
            history = self._alert_history[fingerprint]
            if (now - history.last_seen) < self.dedupe_window:
                history.last_seen = now
                history.count += 1
                return True

        return False

    def _track_alert(self, alert: Alert) -> None:
        """Track alert for deduplication."""
        fingerprint = alert.fingerprint
        now = datetime.now(timezone.utc)

        if fingerprint in self._alert_history:
            history = self._alert_history[fingerprint]
            history.last_seen = now
            history.count += 1
        else:
            self._alert_history[fingerprint] = AlertHistory(
                fingerprint=fingerprint,
                first_seen=now,
                last_seen=now,
            )

        self._cleanup_history()

    def _cleanup_history(self) -> None:
        """Remove old entries from alert history."""
        now = datetime.now(timezone.utc)
        old_fingerprints = [
            fp
            for fp, history in self._alert_history.items()
            if (now - history.last_seen) > self.dedupe_window * 2
        ]
        for fp in old_fingerprints:
            del self._alert_history[fp]

    def _should_notify(
        self,
        alert: Alert,
        min_severity: Optional[AlertSeverity],
    ) -> bool:
        """Check if alert should be sent to a notifier."""
        if min_severity is None:
            return True

        severity_order = [
            AlertSeverity.INFO,
            AlertSeverity.WARNING,
            AlertSeverity.ERROR,
            AlertSeverity.CRITICAL,
        ]

        alert_idx = severity_order.index(alert.severity)
        min_idx = severity_order.index(min_severity)

        return alert_idx >= min_idx


def create_pipeline_failure_alert(
    pipeline_name: str,
    error: str,
    details: Optional[dict] = None,
) -> Alert:
    """Create an alert for pipeline failure."""
    return Alert(
        title=f"Pipeline Failed: {pipeline_name}",
        message=f"The {pipeline_name} pipeline has failed with error: {error}",
        severity=AlertSeverity.ERROR,
        source="pipeline",
        alert_type="pipeline_failure",
        details=details or {},
        tags=["pipeline", "failure"],
    )


def create_data_quality_alert(
    check_name: str,
    message: str,
    severity: AlertSeverity = AlertSeverity.WARNING,
    details: Optional[dict] = None,
) -> Alert:
    """Create an alert for data quality issues."""
    return Alert(
        title=f"Data Quality Issue: {check_name}",
        message=message,
        severity=severity,
        source="data_quality",
        alert_type=f"quality_{check_name}",
        details=details or {},
        tags=["data_quality"],
    )


def create_cost_threshold_alert(
    service: str,
    current_cost: float,
    threshold: float,
    details: Optional[dict] = None,
) -> Alert:
    """Create an alert for cost threshold exceeded."""
    return Alert(
        title=f"Cost Threshold Exceeded: {service}",
        message=f"Cost for {service} (${current_cost:.2f}) has exceeded "
        f"the threshold (${threshold:.2f})",
        severity=AlertSeverity.WARNING,
        source="cost_monitoring",
        alert_type="cost_threshold",
        details=details or {},
        tags=["cost", service],
    )
