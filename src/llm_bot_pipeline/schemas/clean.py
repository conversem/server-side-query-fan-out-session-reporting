"""
Clean table schema for processed LLM bot traffic data.

This schema is the output of the ETL pipeline with enriched fields.
"""

from google.cloud import database

CLEAN_SCHEMA = [
    # Temporal fields
    database.SchemaField("request_timestamp", "TIMESTAMP", mode="REQUIRED"),
    database.SchemaField("request_date", "DATE", mode="REQUIRED"),
    database.SchemaField("request_hour", "INTEGER", mode="REQUIRED"),
    database.SchemaField("day_of_week", "STRING", mode="REQUIRED"),
    # Request fields
    database.SchemaField("request_uri", "STRING", mode="REQUIRED"),
    database.SchemaField("request_host", "STRING", mode="REQUIRED"),
    database.SchemaField("url_path", "STRING", mode="NULLABLE"),
    database.SchemaField("url_path_depth", "INTEGER", mode="NULLABLE"),
    # Bot identification
    database.SchemaField("user_agent_raw", "STRING", mode="NULLABLE"),
    database.SchemaField("bot_name", "STRING", mode="REQUIRED"),
    database.SchemaField("bot_provider", "STRING", mode="REQUIRED"),
    database.SchemaField(
        "bot_category", "STRING", mode="REQUIRED"
    ),  # training | user_request
    database.SchemaField("bot_score", "INTEGER", mode="NULLABLE"),
    database.SchemaField("is_verified_bot", "BOOLEAN", mode="REQUIRED"),
    # Geographic (note: represents crawler server location, not end user)
    database.SchemaField("crawler_country", "STRING", mode="NULLABLE"),
    # Response
    database.SchemaField("response_status", "INTEGER", mode="REQUIRED"),
    database.SchemaField(
        "response_status_category", "STRING", mode="REQUIRED"
    ),  # 2xx, 3xx, etc.
    # Metadata
    database.SchemaField("_processed_at", "TIMESTAMP", mode="REQUIRED"),
]

# Partitioning and clustering configuration for clean table
CLEAN_TABLE_PARTITION_FIELD = "request_date"
CLEAN_TABLE_CLUSTERING_FIELDS = ["bot_provider", "bot_category"]

