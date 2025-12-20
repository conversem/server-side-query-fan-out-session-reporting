"""
Clean/processed table schema for LLM bot traffic.

This schema represents the transformed and enriched data ready for analysis.
"""

# =============================================================================
# Clean Bot Requests Schema (SQLite)
# =============================================================================

CLEAN_BOT_REQUESTS_COLUMNS = {
    # Temporal fields
    "request_timestamp": "TIMESTAMP NOT NULL",
    "request_date": "DATE NOT NULL",
    "request_hour": "INTEGER",
    "day_of_week": "TEXT",
    # Request details
    "request_uri": "TEXT",
    "request_host": "TEXT",
    "url_path": "TEXT",
    "url_path_depth": "INTEGER",
    # Bot classification
    "user_agent_raw": "TEXT",
    "bot_name": "TEXT",
    "bot_provider": "TEXT",
    "bot_category": "TEXT",
    "bot_score": "INTEGER",
    "is_verified_bot": "INTEGER",  # Boolean as 0/1
    # Geo
    "crawler_country": "TEXT",
    # Response
    "response_status": "INTEGER",
    "response_status_category": "TEXT",
    # Metadata
    "_processed_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}


def get_create_clean_table_sql() -> str:
    """Get SQL to create the bot_requests_daily table."""
    columns = ", ".join(
        f"{name} {dtype}" for name, dtype in CLEAN_BOT_REQUESTS_COLUMNS.items()
    )
    return f"CREATE TABLE IF NOT EXISTS bot_requests_daily ({columns})"
