"""Cloudflare Logpull and Logpush integration module.

This module provides Logpull API access for pulling logs into SQLite storage,
and Logpush job management for pushing logs to BigQuery.
"""

from .filters import (
    build_llm_bot_filter,
    build_verified_bot_filter,
    get_filter_json,
    get_llm_bot_user_agent_patterns,
)
from .logpull import (
    IngestionResult,
    LogpullResult,
    RateLimiter,
    check_log_retention,
    estimate_log_volume,
    get_available_date_range,
    ingest_to_sqlite,
    pull_logs,
    pull_logs_for_date_range,
)

try:
    from .logpush_job import (
        LogpushJobResult,
        create_logpush_job,
        delete_logpush_job,
        disable_logpush_job,
        enable_logpush_job,
        find_job_by_name,
        list_logpush_jobs,
        update_logpush_job,
    )
except ImportError:
    pass

__all__ = [
    # Filters
    "build_llm_bot_filter",
    "build_verified_bot_filter",
    "get_filter_json",
    "get_llm_bot_user_agent_patterns",
    # Logpull (local ingestion)
    "LogpullResult",
    "IngestionResult",
    "RateLimiter",
    "pull_logs",
    "pull_logs_for_date_range",
    "ingest_to_sqlite",
    "check_log_retention",
    "get_available_date_range",
    "estimate_log_volume",
    # Logpush (BigQuery push jobs)
    "LogpushJobResult",
    "create_logpush_job",
    "delete_logpush_job",
    "disable_logpush_job",
    "enable_logpush_job",
    "find_job_by_name",
    "list_logpush_jobs",
    "update_logpush_job",
]
