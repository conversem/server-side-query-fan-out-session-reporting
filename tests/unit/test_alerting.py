"""Unit tests for alerting module."""

from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.monitoring.alerting import (
    Alert,
    AlertManager,
    AlertSeverity,
    create_data_quality_alert,
    create_pipeline_failure_alert,
)


class TestAlert:
    """Tests for Alert dataclass."""

    def test_fingerprint_deterministic(self):
        """fingerprint is deterministic for same content."""
        a1 = Alert(
            title="Test",
            message="msg",
            severity=AlertSeverity.WARNING,
            source="pipeline",
            alert_type="failure",
        )
        a2 = Alert(
            title="Test",
            message="msg",
            severity=AlertSeverity.WARNING,
            source="pipeline",
            alert_type="failure",
        )
        assert a1.fingerprint == a2.fingerprint


class TestAlertManager:
    """Tests for AlertManager."""

    def test_alert_manager_sends_notification(self):
        """Mock notifier, assert called on alert."""
        mock_notifier = MagicMock()
        mock_notifier.send.return_value = True

        manager = AlertManager(dedupe_window_minutes=60, max_alerts_per_hour=100)
        manager.add_notifier(mock_notifier)

        alert = Alert(
            title="Pipeline Failed",
            message="ETL failed",
            severity=AlertSeverity.ERROR,
            source="pipeline",
            alert_type="pipeline_failure",
        )

        result = manager.send_alert(alert)

        assert result is True
        mock_notifier.send.assert_called_once_with(alert)

    def test_alert_manager_respects_severity_filter(self):
        """Notifier with min_severity filters lower severity."""
        mock_notifier = MagicMock()
        mock_notifier.send.return_value = True

        manager = AlertManager()
        manager.add_notifier(mock_notifier, min_severity=AlertSeverity.ERROR)

        alert = Alert(
            title="Low severity",
            message="info",
            severity=AlertSeverity.INFO,
            source="test",
            alert_type="info",
        )

        result = manager.send_alert(alert)

        assert result is False
        mock_notifier.send.assert_not_called()

    def test_alert_manager_deduplication(self):
        """Duplicate alerts within window are deduplicated."""
        mock_notifier = MagicMock()
        mock_notifier.send.return_value = True

        manager = AlertManager(dedupe_window_minutes=60, max_alerts_per_hour=100)
        manager.add_notifier(mock_notifier)

        alert = Alert(
            title="Dup",
            message="msg",
            severity=AlertSeverity.WARNING,
            source="test",
            alert_type="dup",
        )

        first = manager.send_alert(alert)
        second = manager.send_alert(alert)

        assert first is True
        assert second is False
        assert mock_notifier.send.call_count == 1


class TestAlertFactories:
    """Tests for alert factory functions."""

    def test_create_pipeline_failure_alert(self):
        """create_pipeline_failure_alert produces correct structure."""
        alert = create_pipeline_failure_alert(
            "etl", "connection timeout", {"detail": "x"}
        )
        assert "etl" in alert.title
        assert "connection timeout" in alert.message
        assert alert.severity == AlertSeverity.ERROR
        assert alert.source == "pipeline"
        assert alert.alert_type == "pipeline_failure"
        assert alert.details["detail"] == "x"

    def test_create_data_quality_alert(self):
        """create_data_quality_alert produces correct structure."""
        alert = create_data_quality_alert(
            "freshness", "Data stale", details={"hours": 25}
        )
        assert "freshness" in alert.title
        assert alert.message == "Data stale"
        assert alert.source == "data_quality"
        assert alert.details["hours"] == 25
