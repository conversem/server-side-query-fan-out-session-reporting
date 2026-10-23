"""Unit tests for pipeline metrics module."""

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from llm_bot_pipeline.monitoring.pipeline_metrics import (
    ExecutionMetrics,
    MetricsCollector,
    PipelineLogger,
    PipelineState,
    get_metrics_collector,
    reset_metrics_collector,
)


class TestExecutionMetrics:
    """Tests for ExecutionMetrics."""

    def test_success_rate_zero_input(self):
        """success_rate is 0 when input_rows is 0."""
        m = ExecutionMetrics(
            execution_id="e1",
            pipeline_name="p1",
            state=PipelineState.RUNNING,
            started_at=datetime.now(timezone.utc),
            input_rows=0,
            output_rows=0,
        )
        assert m.success_rate == 0.0

    def test_success_rate_calculated(self):
        """success_rate = output/input."""
        m = ExecutionMetrics(
            execution_id="e1",
            pipeline_name="p1",
            state=PipelineState.RUNNING,
            started_at=datetime.now(timezone.utc),
            input_rows=100,
            output_rows=95,
            error_rows=5,
        )
        assert m.success_rate == 0.95

    def test_error_rate_calculated(self):
        """error_rate = error_rows/input."""
        m = ExecutionMetrics(
            execution_id="e1",
            pipeline_name="p1",
            state=PipelineState.RUNNING,
            started_at=datetime.now(timezone.utc),
            input_rows=100,
            output_rows=90,
            error_rows=10,
        )
        assert m.error_rate == 0.1

    def test_complete_sets_state_and_duration(self):
        """complete() sets state, completed_at, duration_seconds."""
        m = ExecutionMetrics(
            execution_id="e1",
            pipeline_name="p1",
            state=PipelineState.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        m.complete(PipelineState.SUCCESS)
        assert m.state == PipelineState.SUCCESS
        assert m.completed_at is not None
        assert m.duration_seconds is not None

    def test_to_dict_serializes(self):
        """to_dict produces serializable structure."""
        m = ExecutionMetrics(
            execution_id="e1",
            pipeline_name="p1",
            state=PipelineState.SUCCESS,
            started_at=datetime.now(timezone.utc),
            input_rows=10,
            output_rows=10,
        )
        m.complete(PipelineState.SUCCESS)
        d = m.to_dict()
        assert d["execution_id"] == "e1"
        assert d["pipeline_name"] == "p1"
        assert d["state"] == "success"
        assert d["success_rate"] == 1.0


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_start_execution_returns_metrics(self):
        """start_execution returns ExecutionMetrics in RUNNING state."""
        collector = MetricsCollector()
        m = collector.start_execution("test_pipeline", {"param": "val"})
        assert m.pipeline_name == "test_pipeline"
        assert m.state == PipelineState.RUNNING
        assert m.parameters == {"param": "val"}

    def test_complete_execution_stores_in_history(self):
        """complete_execution stores metrics in history."""
        collector = MetricsCollector()
        m = collector.start_execution("test_pipeline")
        collector.complete_execution(m, success=True)
        history = collector.get_execution_history(limit=5)
        assert len(history) == 1
        assert history[0].state == PipelineState.SUCCESS

    def test_get_statistics_aggregates(self):
        """get_statistics returns aggregate stats."""
        collector = MetricsCollector()
        m1 = collector.start_execution("p1")
        m1.input_rows = 100
        m1.output_rows = 95
        collector.complete_execution(m1, success=True)

        m2 = collector.start_execution("p1")
        m2.input_rows = 50
        m2.output_rows = 50
        collector.complete_execution(m2, success=True)

        stats = collector.get_statistics(pipeline_name="p1")
        assert stats["total_executions"] == 2
        assert stats["success_count"] == 2
        assert stats["total_input_rows"] == 150
        assert stats["total_output_rows"] == 145

    def test_export_no_execution_returns_quietly(self):
        """export() with no execution and empty history does not raise."""
        collector = MetricsCollector()
        collector.export(execution=None, enabled=False)

    def test_export_enabled_false_skips(self):
        """export() with enabled=False does not call export_metrics."""
        collector = MetricsCollector()
        m = collector.start_execution("p1")
        m.output_rows = 10
        m.error_rows = 0
        m.duration_seconds = 5.0
        collector.complete_execution(m, success=True)

        with patch(
            "llm_bot_pipeline.monitoring.metrics_export.export_metrics"
        ) as mock_export:
            collector.export(execution=m, enabled=False)
            mock_export.assert_not_called()

    def test_export_prometheus_calls_export_metrics(self):
        """export() with enabled=True and backend=prometheus calls export_metrics."""
        collector = MetricsCollector()
        m = collector.start_execution("p1")
        m.output_rows = 100
        m.error_rows = 2
        m.duration_seconds = 12.5
        m.custom_metrics = {"batch_flush_errors": 1}
        collector.complete_execution(m, success=True)

        with patch(
            "llm_bot_pipeline.monitoring.metrics_export.export_metrics"
        ) as mock_export:
            collector.export(
                execution=m,
                enabled=True,
                backend="prometheus",
                project_id="",
                pushgateway_url="http://localhost:9091",
            )
            mock_export.assert_called_once()
            call_args = mock_export.call_args
            assert call_args[0][1] == "prometheus"
            assert call_args[1]["pushgateway_url"] == "http://localhost:9091"

    def test_export_failure_does_not_raise(self):
        """export() catches exceptions and logs; does not raise."""
        collector = MetricsCollector()
        m = collector.start_execution("p1")
        m.output_rows = 10
        collector.complete_execution(m, success=True)

        with patch(
            "llm_bot_pipeline.monitoring.metrics_export.export_metrics",
            side_effect=RuntimeError("network error"),
        ):
            collector.export(
                execution=m,
                enabled=True,
                backend="prometheus",
                project_id="",
                pushgateway_url="http://localhost:9091",
            )
        # No exception raised


class TestPipelineLogger:
    """Tests for PipelineLogger."""

    def test_info_logs_with_context(self):
        """info() passes extra context to logger."""
        recorded = []

        class CaptureHandler(logging.Handler):
            def emit(self, record):
                recorded.append(record)

        handler = CaptureHandler()
        handler.setLevel(logging.DEBUG)
        pl = PipelineLogger("test_pipeline", execution_id="abc123")
        pl.logger.addHandler(handler)
        pl.logger.setLevel(logging.DEBUG)

        pl.info("test message", stage="extract")

        assert len(recorded) == 1
        assert recorded[0].msg == "test message"
        assert getattr(recorded[0], "stage", None) == "extract"

    def test_set_context_updates(self):
        """set_context updates internal context."""
        logger = PipelineLogger("test_pipeline")
        logger.set_context(stage="load", rows=100)
        assert logger._context["stage"] == "load"
        assert logger._context["rows"] == 100


class TestGlobalMetricsCollector:
    """Tests for get_metrics_collector and reset."""

    def teardown_method(self):
        """Reset global collector after each test."""
        reset_metrics_collector()

    def test_get_metrics_collector_returns_singleton(self):
        """get_metrics_collector returns same instance."""
        reset_metrics_collector()
        c1 = get_metrics_collector()
        c2 = get_metrics_collector()
        assert c1 is c2
