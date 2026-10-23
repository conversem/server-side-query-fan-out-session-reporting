"""Unit tests for metrics export module."""

from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.monitoring.metrics_export import (
    BATCH_FLUSH_ERRORS,
    PIPELINE_DURATION_SECONDS,
    RECORDS_FAILED,
    RECORDS_PROCESSED,
    _extract_metrics,
    export_metrics,
)


class TestExtractMetrics:
    """Tests for _extract_metrics."""

    def test_extracts_from_execution_metrics(self):
        """Extracts records_processed, records_failed, duration, batch_flush_errors."""
        execution = MagicMock()
        execution.output_rows = 100
        execution.input_rows = 105
        execution.error_rows = 5
        execution.duration_seconds = 12.5
        execution.custom_metrics = {"batch_flush_errors": 2}

        m = _extract_metrics(execution)
        assert m[RECORDS_PROCESSED] == 100
        assert m[RECORDS_FAILED] == 5
        assert m[PIPELINE_DURATION_SECONDS] == 12.5
        assert m[BATCH_FLUSH_ERRORS] == 2

    def test_uses_input_rows_when_output_zero(self):
        """Uses input_rows when output_rows is 0."""
        execution = MagicMock()
        execution.output_rows = 0
        execution.input_rows = 50
        execution.error_rows = 10
        execution.duration_seconds = 5.0
        execution.custom_metrics = {}

        m = _extract_metrics(execution)
        assert m[RECORDS_PROCESSED] == 50
        assert m[BATCH_FLUSH_ERRORS] == 0

    def test_none_execution_returns_early(self):
        """export_metrics with None execution returns without error."""
        with patch(
            "llm_bot_pipeline.monitoring.metrics_export._extract_metrics"
        ) as mock_extract:
            export_metrics(None, "prometheus")
            mock_extract.assert_not_called()


class TestExportMetrics:
    """Tests for export_metrics."""

    def test_prometheus_export(self):
        """Prometheus backend pushes to gateway."""
        pytest.importorskip("prometheus_client", reason="prometheus-client optional")
        execution = MagicMock()
        execution.output_rows = 10
        execution.input_rows = 10
        execution.error_rows = 0
        execution.duration_seconds = 1.0
        execution.custom_metrics = {}
        execution.pipeline_name = "test_pipeline"

        with patch("prometheus_client.push_to_gateway") as mock_push:
            export_metrics(
                execution,
                "prometheus",
                pushgateway_url="http://push:9091",
            )
            mock_push.assert_called_once()
            assert mock_push.call_args[0][0] == "http://push:9091"

    def test_unknown_backend_logs_warning(self, caplog):
        """Unknown backend logs warning and returns."""
        execution = MagicMock()
        execution.output_rows = 10
        execution.pipeline_name = "p1"

        with caplog.at_level("WARNING"):
            export_metrics(execution, "unknown_backend")
        assert "Unknown metrics backend" in caplog.text
