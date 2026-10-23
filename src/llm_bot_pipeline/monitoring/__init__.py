"""Pipeline monitoring module.

Provides:
- Retry handling with circuit breaker
- Pipeline metrics and execution tracking
- Data quality checks (freshness, schema, duplicates) — backend-agnostic
- Alerting (email, webhooks, Slack/Discord)
- Pipeline scheduling (APScheduler-based)
"""

from .alerting import AlertManager, AlertSeverity
from .data_quality import DataQualityChecker
from .pipeline_metrics import (
    ExecutionTracker,
    MetricsCollector,
    PipelineLogger,
    reset_metrics_collector,
)
from .retry_handler import RetryConfig
from .retry_handler import RetryManager as RetryHandler
from .retry_handler import with_retry

__all__ = [
    "RetryConfig",
    "RetryHandler",
    "with_retry",
    "PipelineLogger",
    "MetricsCollector",
    "ExecutionTracker",
    "reset_metrics_collector",
    "AlertManager",
    "AlertSeverity",
    "DataQualityChecker",
]

# Scheduler (requires apscheduler)
try:
    from .scheduler import PipelineScheduler, ScheduleConfig

    __all__.extend(["PipelineScheduler", "ScheduleConfig"])
except ImportError:
    pass
