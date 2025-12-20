"""
Reporting table schemas for aggregated LLM bot traffic data.

These schemas define the structure for summary and analytical tables.
"""

# =============================================================================
# Daily Summary Schema (SQLite)
# =============================================================================

DAILY_SUMMARY_COLUMNS = {
    "summary_date": "DATE PRIMARY KEY",
    "total_requests": "INTEGER",
    "unique_bots": "INTEGER",
    "unique_ips": "INTEGER",
    "unique_urls": "INTEGER",
    "avg_bot_score": "REAL",
    "verified_bot_pct": "REAL",
    "success_rate": "REAL",
    "_created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}


# =============================================================================
# URL Performance Schema (SQLite)
# =============================================================================

URL_PERFORMANCE_COLUMNS = {
    "url_path": "TEXT PRIMARY KEY",
    "request_count": "INTEGER",
    "unique_bots": "INTEGER",
    "avg_bot_score": "REAL",
    "success_rate": "REAL",
    "first_seen": "DATE",
    "last_seen": "DATE",
    "_created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}


# =============================================================================
# Bot Provider Summary Schema (SQLite)
# =============================================================================

BOT_PROVIDER_SUMMARY_COLUMNS = {
    "summary_date": "DATE",
    "bot_provider": "TEXT",
    "request_count": "INTEGER",
    "unique_urls": "INTEGER",
    "avg_requests_per_session": "REAL",
    "session_count": "INTEGER",
    "_created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}


def get_create_daily_summary_sql() -> str:
    """Get SQL to create the daily_summary table."""
    columns = ", ".join(
        f"{name} {dtype}" for name, dtype in DAILY_SUMMARY_COLUMNS.items()
    )
    return f"CREATE TABLE IF NOT EXISTS daily_summary ({columns})"


def get_create_url_performance_sql() -> str:
    """Get SQL to create the url_performance table."""
    columns = ", ".join(
        f"{name} {dtype}" for name, dtype in URL_PERFORMANCE_COLUMNS.items()
    )
    return f"CREATE TABLE IF NOT EXISTS url_performance ({columns})"


def get_create_bot_provider_summary_sql() -> str:
    """Get SQL to create the bot_provider_summary table."""
    columns = ", ".join(
        f"{name} {dtype}" for name, dtype in BOT_PROVIDER_SUMMARY_COLUMNS.items()
    )
    return f"CREATE TABLE IF NOT EXISTS bot_provider_summary ({columns})"
