"""
Raw table schema for Cloudflare log data.

This schema matches the fields selected in the log ingestion configuration.
"""

# =============================================================================
# Raw Bot Requests Schema (SQLite)
# =============================================================================

RAW_BOT_REQUESTS_COLUMNS = {
    "EdgeStartTimestamp": "TIMESTAMP",
    "ClientRequestURI": "TEXT",
    "ClientRequestHost": "TEXT",
    "ClientRequestUserAgent": "TEXT",
    "BotScore": "INTEGER",
    "BotScoreSrc": "TEXT",
    "VerifiedBot": "INTEGER",  # Boolean as 0/1
    "BotTags": "TEXT",  # JSON array
    "ClientIP": "TEXT",
    "ClientCountry": "TEXT",
    "EdgeResponseStatus": "INTEGER",
    "RayID": "TEXT",
    # Metadata
    "_ingested_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    # Data provenance
    "source_provider": "TEXT",  # Provider that ingested this record
}


def get_create_raw_table_sql() -> str:
    """Get SQL to create the raw_bot_requests table."""
    columns = ", ".join(
        f"{name} {dtype}" for name, dtype in RAW_BOT_REQUESTS_COLUMNS.items()
    )
    return f"CREATE TABLE IF NOT EXISTS raw_bot_requests ({columns})"
