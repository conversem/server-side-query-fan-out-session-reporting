"""Schemas for LLM bot traffic data storage."""

from .bundles import (
    QUERY_FANOUT_SESSIONS_COLUMNS,
    QueryFanoutSession,
    get_create_sessions_table_sql,
)
from .clean import (
    CLEAN_BOT_REQUESTS_COLUMNS,
    get_create_clean_table_sql,
)
from .raw import (
    RAW_BOT_REQUESTS_COLUMNS,
    get_create_raw_table_sql,
)
from .reporting import (
    BOT_PROVIDER_SUMMARY_COLUMNS,
    DAILY_SUMMARY_COLUMNS,
    URL_PERFORMANCE_COLUMNS,
    get_create_bot_provider_summary_sql,
    get_create_daily_summary_sql,
    get_create_url_performance_sql,
)

__all__ = [
    # Raw schema
    "RAW_BOT_REQUESTS_COLUMNS",
    "get_create_raw_table_sql",
    # Clean schema
    "CLEAN_BOT_REQUESTS_COLUMNS",
    "get_create_clean_table_sql",
    # Session bundles
    "QUERY_FANOUT_SESSIONS_COLUMNS",
    "QueryFanoutSession",
    "get_create_sessions_table_sql",
    # Reporting
    "DAILY_SUMMARY_COLUMNS",
    "URL_PERFORMANCE_COLUMNS",
    "BOT_PROVIDER_SUMMARY_COLUMNS",
    "get_create_daily_summary_sql",
    "get_create_url_performance_sql",
    "get_create_bot_provider_summary_sql",
]
