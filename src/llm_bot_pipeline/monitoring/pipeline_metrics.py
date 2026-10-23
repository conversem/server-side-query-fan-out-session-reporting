"""
Pipeline metrics and logging system.

Provides comprehensive execution tracking:
- Structured logging with correlation IDs
- Metrics collection (timing, counts, rates)
- Pipeline state management
- Execution history tracking
"""

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class PipelineState(Enum):
    """Pipeline execution states."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ExecutionMetrics:
    """Metrics for a single pipeline execution."""

    execution_id: str
    pipeline_name: str
    state: PipelineState
    started_at: datetime
    completed_at: Optional[datetime] = None
    input_rows: int = 0
    output_rows: int = 0
    error_rows: int = 0
    duration_seconds: Optional[float] = None
    stage_timings: dict[str, float] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    custom_metrics: dict[str, Any] = field(default_factory=dict)

    def complete(self, state: PipelineState) -> None:
        """Mark execution as complete."""
        self.state = state
        self.completed_at = datetime.now(timezone.utc)
        self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

    @property
    def success_rate(self) -> float:
        """Calculate success rate (output/input)."""
        if self.input_rows == 0:
            return 0.0
        return self.output_rows / self.input_rows

    @property
    def error_rate(self) -> float:
        """Calculate error rate (errors/input)."""
        if self.input_rows == 0:
            return 0.0
        return self.error_rows / self.input_rows

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "execution_id": self.execution_id,
            "pipeline_name": self.pipeline_name,
            "state": self.state.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration_seconds": self.duration_seconds,
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "error_rows": self.error_rows,
            "success_rate": round(self.success_rate, 4),
            "error_rate": round(self.error_rate, 4),
            "stage_timings": self.stage_timings,
            "parameters": self.parameters,
            "errors": self.errors,
            "warnings": self.warnings,
            "custom_metrics": self.custom_metrics,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class PipelineLogger:
    """
    Structured logger for pipeline operations.

    Provides JSON-formatted logs with correlation IDs and context.
    """

    def __init__(
        self,
        pipeline_name: str,
        execution_id: Optional[str] = None,
        log_level: int = logging.INFO,
    ):
        """
        Initialize pipeline logger.

        Args:
            pipeline_name: Name of the pipeline
            execution_id: Unique execution ID (auto-generated if None)
            log_level: Logging level
        """
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id or str(uuid.uuid4())[:8]
        self.logger = logging.getLogger(f"pipeline.{pipeline_name}")
        self.logger.setLevel(log_level)
        self._context: dict[str, Any] = {}

    def set_context(self, **kwargs) -> None:
        """Set additional context for all log messages."""
        self._context.update(kwargs)

    def clear_context(self) -> None:
        """Clear context."""
        self._context.clear()

    def _build_extra(self, **kwargs) -> dict:
        """Build extra dict for structured logging without double-encoding."""
        stage = kwargs.get("stage") or self._context.get("stage")
        extra = {
            "execution_id": self.execution_id,
            "pipeline_data": {
                "execution_id": self.execution_id,
                "pipeline": self.pipeline_name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **self._context,
                **kwargs,
            },
        }
        if stage is not None:
            extra["stage"] = stage
        return extra

    def info(self, message: str, **kwargs) -> None:
        """Log info message with context."""
        self.logger.info(message, extra=self._build_extra(**kwargs))

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with context."""
        self.logger.warning(message, extra=self._build_extra(**kwargs))

    def error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log error message with context and optional exception."""
        extra_kwargs = dict(kwargs)
        if error:
            extra_kwargs["error_type"] = type(error).__name__
            extra_kwargs["error_message"] = str(error)
        self.logger.error(message, extra=self._build_extra(**extra_kwargs))

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with context."""
        self.logger.debug(message, extra=self._build_extra(**kwargs))

    def stage_start(self, stage_name: str) -> None:
        """Log start of a pipeline stage."""
        self.info(
            f"Stage started: {stage_name}",
            stage=stage_name,
            event="stage_start",
        )

    def stage_complete(
        self, stage_name: str, duration_seconds: float, **metrics
    ) -> None:
        """Log completion of a pipeline stage."""
        self.info(
            f"Stage completed: {stage_name} ({duration_seconds:.2f}s)",
            stage=stage_name,
            event="stage_complete",
            duration_seconds=round(duration_seconds, 3),
            **metrics,
        )

    def stage_failed(
        self, stage_name: str, error: Exception, duration_seconds: float
    ) -> None:
        """Log failure of a pipeline stage."""
        self.error(
            f"Stage failed: {stage_name}",
            error=error,
            stage=stage_name,
            event="stage_failed",
            duration_seconds=round(duration_seconds, 3),
        )


class MetricsCollector:
    """
    Collects and aggregates pipeline metrics.

    Tracks execution statistics over time for monitoring and alerting.
    """

    def __init__(self, max_history: int = 100):
        """
        Initialize metrics collector.

        Args:
            max_history: Maximum number of executions to keep in history
        """
        self.max_history = max_history
        self._lock = threading.Lock()
        self._executions: list[ExecutionMetrics] = []
        self._current_execution: Optional[ExecutionMetrics] = None

    def start_execution(
        self,
        pipeline_name: str,
        parameters: Optional[dict] = None,
    ) -> ExecutionMetrics:
        """
        Start tracking a new pipeline execution.

        Args:
            pipeline_name: Name of the pipeline
            parameters: Execution parameters

        Returns:
            ExecutionMetrics instance for tracking
        """
        execution = ExecutionMetrics(
            execution_id=str(uuid.uuid4())[:8],
            pipeline_name=pipeline_name,
            state=PipelineState.RUNNING,
            started_at=datetime.now(timezone.utc),
            parameters=parameters or {},
        )
        with self._lock:
            self._current_execution = execution
        return execution

    def complete_execution(
        self,
        execution: ExecutionMetrics,
        success: bool = True,
    ) -> None:
        """
        Mark execution as complete and store in history.

        Args:
            execution: ExecutionMetrics instance
            success: Whether execution was successful
        """
        state = PipelineState.SUCCESS if success else PipelineState.FAILED
        execution.complete(state)

        with self._lock:
            self._executions.append(execution)
            self._current_execution = None

            if len(self._executions) > self.max_history:
                self._executions = self._executions[-self.max_history :]

        self._export_if_enabled(execution)

    def export(
        self,
        execution: Optional[ExecutionMetrics] = None,
        *,
        enabled: Optional[bool] = None,
        backend: Optional[str] = None,
        project_id: Optional[str] = None,
        pushgateway_url: Optional[str] = None,
    ) -> None:
        """
        Export metrics to Cloud Monitoring or Prometheus push gateway.

        Non-blocking: logs warnings on failure, never raises.

        Args:
            execution: ExecutionMetrics to export (default: latest from history)
            enabled: Override settings; if False, skip export
            backend: Override backend ("prometheus" | "cloud_monitoring")
            project_id: GCP project ID (for cloud_monitoring)
            pushgateway_url: Prometheus push gateway URL
        """
        try:
            from .metrics_export import export_metrics

            target = execution
            if target is None:
                history = self.get_execution_history(limit=1)
                target = history[0] if history else None
            if target is None:
                return

            if enabled is None or backend is None or pushgateway_url is None:
                try:
                    from ..config.settings import get_settings

                    settings = get_settings()
                    if enabled is None:
                        enabled = getattr(settings, "metrics_export_enabled", False)
                    if backend is None:
                        backend = getattr(settings, "metrics_backend", "prometheus")
                    if project_id is None:
                        project_id = getattr(settings, "gcp_project_id", "") or ""
                    if pushgateway_url is None:
                        pushgateway_url = getattr(
                            settings,
                            "metrics_pushgateway_url",
                            "http://localhost:9091",
                        )
                except Exception:
                    if enabled is None:
                        enabled = False
                    if backend is None:
                        backend = "prometheus"
                    if project_id is None:
                        project_id = ""
                    if pushgateway_url is None:
                        pushgateway_url = "http://localhost:9091"

            if not enabled:
                return

            export_metrics(
                target,
                backend,
                project_id=project_id or "",
                pushgateway_url=pushgateway_url,
            )
        except Exception as e:
            logger.warning(
                "Metrics export failed (non-blocking): %s",
                e,
                exc_info=False,
            )

    def _export_if_enabled(self, execution: ExecutionMetrics) -> None:
        """Export metrics to configured backend if enabled. Non-blocking."""
        self.export(execution=execution)

    def get_current_execution(self) -> Optional[ExecutionMetrics]:
        """Get currently running execution if any."""
        return self._current_execution

    def get_execution_history(
        self,
        pipeline_name: Optional[str] = None,
        limit: int = 10,
    ) -> list[ExecutionMetrics]:
        """
        Get recent execution history.

        Args:
            pipeline_name: Filter by pipeline name
            limit: Maximum executions to return

        Returns:
            List of ExecutionMetrics
        """
        executions = self._executions
        if pipeline_name:
            executions = [e for e in executions if e.pipeline_name == pipeline_name]
        return executions[-limit:]

    def get_statistics(
        self,
        pipeline_name: Optional[str] = None,
        lookback_hours: int = 24,
    ) -> dict:
        """
        Calculate aggregate statistics over recent executions.

        Args:
            pipeline_name: Filter by pipeline name
            lookback_hours: Hours to look back

        Returns:
            Dictionary with aggregate statistics
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        with self._lock:
            executions = [
                e
                for e in self._executions
                if e.started_at >= cutoff
                and (pipeline_name is None or e.pipeline_name == pipeline_name)
            ]

        if not executions:
            return {
                "total_executions": 0,
                "success_count": 0,
                "failure_count": 0,
                "success_rate": 0.0,
                "avg_duration_seconds": 0.0,
                "total_input_rows": 0,
                "total_output_rows": 0,
            }

        success_count = sum(1 for e in executions if e.state == PipelineState.SUCCESS)
        durations = [
            e.duration_seconds for e in executions if e.duration_seconds is not None
        ]

        return {
            "total_executions": len(executions),
            "success_count": success_count,
            "failure_count": len(executions) - success_count,
            "success_rate": (success_count / len(executions) if executions else 0.0),
            "avg_duration_seconds": (
                sum(durations) / len(durations) if durations else 0.0
            ),
            "min_duration_seconds": min(durations) if durations else 0.0,
            "max_duration_seconds": max(durations) if durations else 0.0,
            "total_input_rows": sum(e.input_rows for e in executions),
            "total_output_rows": sum(e.output_rows for e in executions),
            "total_error_rows": sum(e.error_rows for e in executions),
            "lookback_hours": lookback_hours,
        }


class ExecutionTracker:
    """
    High-level execution tracker combining logging and metrics.

    Provides a convenient context manager for pipeline execution tracking.
    """

    def __init__(
        self,
        pipeline_name: str,
        metrics_collector: Optional[MetricsCollector] = None,
        log_level: int = logging.INFO,
    ):
        """
        Initialize execution tracker.

        Args:
            pipeline_name: Name of the pipeline
            metrics_collector: Optional shared metrics collector
            log_level: Logging level
        """
        self.pipeline_name = pipeline_name
        self.metrics_collector = metrics_collector or MetricsCollector()
        self.log_level = log_level
        self._execution: Optional[ExecutionMetrics] = None
        self._logger: Optional[PipelineLogger] = None
        self._stage_start_time: Optional[datetime] = None
        self._current_stage: Optional[str] = None

    def __enter__(self) -> "ExecutionTracker":
        """Start tracking execution."""
        self._execution = self.metrics_collector.start_execution(self.pipeline_name)
        self._logger = PipelineLogger(
            pipeline_name=self.pipeline_name,
            execution_id=self._execution.execution_id,
            log_level=self.log_level,
        )
        from ..config.logging_config import set_log_context

        set_log_context(execution_id=self._execution.execution_id)
        self._logger.info("Pipeline execution started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Complete execution tracking."""
        if exc_type is not None:
            self.record_error(exc_val)
            self.metrics_collector.complete_execution(self._execution, success=False)
            self._logger.error("Pipeline execution failed", error=exc_val)
        else:
            self.metrics_collector.complete_execution(self._execution, success=True)
            self._logger.info(
                "Pipeline execution completed",
                duration_seconds=self._execution.duration_seconds,
                output_rows=self._execution.output_rows,
            )
        return False

    @property
    def execution(self) -> ExecutionMetrics:
        """Get current execution metrics."""
        if self._execution is None:
            raise RuntimeError("Execution not started. Use context manager.")
        return self._execution

    @property
    def logger(self) -> PipelineLogger:
        """Get pipeline logger."""
        if self._logger is None:
            raise RuntimeError("Logger not initialized. Use context manager.")
        return self._logger

    def set_parameters(self, **kwargs) -> None:
        """Set execution parameters."""
        self.execution.parameters.update(kwargs)
        self.logger.set_context(**kwargs)

    def start_stage(self, stage_name: str) -> None:
        """Start tracking a pipeline stage."""
        self._stage_start_time = datetime.now(timezone.utc)
        self._current_stage = stage_name
        self.logger.stage_start(stage_name)

    def complete_stage(self, **metrics) -> None:
        """Complete current pipeline stage."""
        if self._stage_start_time and self._current_stage:
            duration = (
                datetime.now(timezone.utc) - self._stage_start_time
            ).total_seconds()
            self.execution.stage_timings[self._current_stage] = duration
            self.logger.stage_complete(self._current_stage, duration, **metrics)
            self._current_stage = None
            self._stage_start_time = None

    def fail_stage(self, error: Exception) -> None:
        """Mark current stage as failed."""
        if self._stage_start_time and self._current_stage:
            duration = (
                datetime.now(timezone.utc) - self._stage_start_time
            ).total_seconds()
            self.execution.stage_timings[self._current_stage] = duration
            self.logger.stage_failed(self._current_stage, error, duration)
            self.record_error(error)
            self._current_stage = None
            self._stage_start_time = None

    def record_rows(
        self,
        input_rows: Optional[int] = None,
        output_rows: Optional[int] = None,
        error_rows: Optional[int] = None,
    ) -> None:
        """Record row counts."""
        if input_rows is not None:
            self.execution.input_rows = input_rows
        if output_rows is not None:
            self.execution.output_rows = output_rows
        if error_rows is not None:
            self.execution.error_rows = error_rows

    def record_metric(self, name: str, value: Any) -> None:
        """Record a custom metric."""
        self.execution.custom_metrics[name] = value

    def record_error(self, error: Exception, context: Optional[dict] = None) -> None:
        """Record an error."""
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stage": self._current_stage,
        }
        if context:
            error_info["context"] = context
        self.execution.errors.append(error_info)

    def record_warning(self, message: str) -> None:
        """Record a warning."""
        self.execution.warnings.append(message)
        self.logger.warning(message)


_global_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get or create global metrics collector."""
    global _global_collector
    if _global_collector is None:
        _global_collector = MetricsCollector()
    return _global_collector


def reset_metrics_collector() -> None:
    """Reset the global metrics collector (for testing)."""
    global _global_collector
    _global_collector = None


def track_execution(pipeline_name: str) -> ExecutionTracker:
    """
    Create an execution tracker using global metrics collector.

    Args:
        pipeline_name: Name of the pipeline

    Returns:
        ExecutionTracker instance

    Example:
        with track_execution("etl_pipeline") as tracker:
            tracker.start_stage("extract")
            data = extract_data()
            tracker.complete_stage(rows=len(data))
    """
    return ExecutionTracker(
        pipeline_name=pipeline_name,
        metrics_collector=get_metrics_collector(),
    )
