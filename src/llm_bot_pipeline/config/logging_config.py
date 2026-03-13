"""
Structured logging configuration for cloud and local environments.

Provides:
- JSON formatter for cloud environments (Cloud Logging compatible)
- Human-readable formatter for local development
- Context propagation for execution_id and stage via contextvars
"""

import json
import logging
import os
from contextvars import ContextVar
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

# Context variables for log correlation (execution_id, stage)
_execution_id_var: ContextVar[Optional[str]] = ContextVar("execution_id", default=None)
_stage_var: ContextVar[Optional[str]] = ContextVar("stage", default=None)


def set_log_context(
    execution_id: Optional[str] = None,
    stage: Optional[str] = None,
) -> None:
    """Set execution_id and/or stage for log correlation.

    These values are propagated to all log records in the current context
    when JSON logging is enabled.
    """
    if execution_id is not None:
        _execution_id_var.set(execution_id)
    if stage is not None:
        _stage_var.set(stage)


def get_log_context() -> tuple[Optional[str], Optional[str]]:
    """Get current execution_id and stage from context."""
    return (_execution_id_var.get(), _stage_var.get())


def clear_log_context() -> None:
    """Clear execution_id and stage from context."""
    try:
        _execution_id_var.set(None)
    except LookupError:
        pass
    try:
        _stage_var.set(None)
    except LookupError:
        pass


def build_log_context(
    date_range: Optional[tuple[Union[date, str], Union[date, str]]] = None,
    batch_size: Optional[int] = None,
    records_processed: Optional[int] = None,
    execution_id: Optional[str] = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build extra dict for log records with contextual debugging fields.

    Args:
        date_range: Optional (start_date, end_date) tuple for date range context.
        batch_size: Optional batch size for batch-processing context.
        records_processed: Optional count of records processed so far.
        execution_id: Optional execution ID; falls back to context if not set.
        **extra: Additional key-value pairs to include in context.

    Returns:
        Dict suitable for logger.log(..., extra=...).
    """
    context: dict[str, Any] = {}
    if date_range:
        context["date_range"] = f"{date_range[0]} to {date_range[1]}"
    if batch_size is not None:
        context["batch_size"] = batch_size
    if records_processed is not None:
        context["records_processed"] = records_processed
    exec_id = execution_id if execution_id is not None else get_log_context()[0]
    if exec_id is not None:
        context["execution_id"] = exec_id
    context.update(extra)
    return context


def log_with_context(
    logger_instance: logging.Logger,
    level: int,
    message: str,
    *args: Any,
    exc_info: bool = False,
    **context_kwargs: Any,
) -> None:
    """Log with contextual debugging fields (date_range, batch_size, etc.).

    Pass date_range, batch_size, records_processed, execution_id via
    context_kwargs. These are included in the log record's extra dict
    for JSON logging and structured debugging.
    """
    extra = build_log_context(**context_kwargs)
    logger_instance.log(level, message, *args, extra=extra, exc_info=exc_info)


class ContextFilter(logging.Filter):
    """Filter that adds execution_id and stage from context to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "execution_id"):
            record.execution_id = _execution_id_var.get()
        if not hasattr(record, "stage"):
            record.stage = _stage_var.get()
        return True


class JsonFormatter(logging.Formatter):
    """Format log records as JSON for cloud environments.

    Output fields: timestamp, level, logger, message, execution_id, stage,
    plus any extra attributes on the record.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_dict: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        execution_id = getattr(record, "execution_id", None)
        if execution_id is not None:
            log_dict["execution_id"] = execution_id

        stage = getattr(record, "stage", None)
        if stage is not None:
            log_dict["stage"] = stage

        # Include standard record attributes that might be useful
        if record.exc_info:
            log_dict["exception"] = self.formatException(record.exc_info)

        # Include extra fields (skip internal logging attributes)
        skip = {
            "name",
            "msg",
            "args",
            "created",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "exc_info",
            "exc_text",
            "thread",
            "threadName",
            "message",
            "asctime",
            "execution_id",
            "stage",
        }
        for key, value in record.__dict__.items():
            if key not in skip and value is not None:
                log_dict[key] = value

        return json.dumps(log_dict, default=str)


def _resolve_json_logs(json_logs: Optional[bool] = None) -> bool:
    """Resolve json_logs from argument, JSON_LOGS env, or Settings."""
    if json_logs is not None:
        return bool(json_logs)
    if os.environ.get("JSON_LOGS", "").lower() in ("true", "1", "yes"):
        return True
    try:
        from .settings import get_settings

        return get_settings().json_logs
    except Exception:
        return False


def setup_logging(
    level: int = logging.INFO,
    json_logs: Optional[bool] = None,
) -> None:
    """Configure logging for the pipeline.

    Args:
        level: Logging level (default: INFO)
        json_logs: Use JSON formatter when True. When None, reads from
            JSON_LOGS env var. Default (False) preserves human-readable format.
    """
    use_json = _resolve_json_logs(json_logs)

    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates on repeated setup
    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    if use_json:
        handler.setFormatter(JsonFormatter())
        handler.addFilter(ContextFilter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root.addHandler(handler)


def setup_file_logging(
    log_dir: "os.PathLike[str]",
    verbose: bool = False,
    json_logs: Optional[bool] = None,
) -> Path:
    """Configure logging to both console and file.

    Returns:
        Path to the log file.
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"daily_etl_{timestamp}.log"

    use_json = _resolve_json_logs(json_logs)
    log_level = logging.DEBUG if verbose else logging.INFO

    root = logging.getLogger()
    root.setLevel(log_level)

    # Remove existing handlers
    for h in root.handlers[:]:
        root.removeHandler(h)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    if use_json:
        console_handler.setFormatter(JsonFormatter())
        console_handler.addFilter(ContextFilter())
    else:
        console_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    root.addHandler(console_handler)

    # File handler (always human-readable for local inspection, or JSON if requested)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    if use_json:
        file_handler.setFormatter(JsonFormatter())
        file_handler.addFilter(ContextFilter())
    else:
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    root.addHandler(file_handler)

    return log_file
