"""
Reporting table schemas for aggregated LLM bot traffic data.

These schemas define pre-aggregated tables optimized for dashboard queries.
"""

from google.cloud import database

# Daily Summary Table
# Aggregates bot traffic by date and bot characteristics
DAILY_SUMMARY_SCHEMA = [
    # Dimensions
    database.SchemaField("request_date", "DATE", mode="REQUIRED"),
    database.SchemaField("bot_provider", "STRING", mode="REQUIRED"),
    database.SchemaField("bot_name", "STRING", mode="REQUIRED"),
    database.SchemaField("bot_category", "STRING", mode="REQUIRED"),  # training | user_request
    # Metrics
    database.SchemaField("total_requests", "INTEGER", mode="REQUIRED"),
    database.SchemaField("unique_urls", "INTEGER", mode="REQUIRED"),
    database.SchemaField("unique_hosts", "INTEGER", mode="REQUIRED"),
    database.SchemaField("avg_bot_score", "FLOAT64", mode="NULLABLE"),
    database.SchemaField("successful_requests", "INTEGER", mode="REQUIRED"),  # 2xx responses
    database.SchemaField("error_requests", "INTEGER", mode="REQUIRED"),  # 4xx + 5xx responses
    database.SchemaField("redirect_requests", "INTEGER", mode="REQUIRED"),  # 3xx responses
    # Metadata
    database.SchemaField("_aggregated_at", "TIMESTAMP", mode="REQUIRED"),
]

DAILY_SUMMARY_PARTITION_FIELD = "request_date"
DAILY_SUMMARY_CLUSTERING_FIELDS = ["bot_provider", "bot_category"]


# URL Performance Table
# Aggregates bot traffic by URL for content analysis
URL_PERFORMANCE_SCHEMA = [
    # Dimensions
    database.SchemaField("request_date", "DATE", mode="REQUIRED"),
    database.SchemaField("request_host", "STRING", mode="REQUIRED"),
    database.SchemaField("url_path", "STRING", mode="REQUIRED"),
    # Bot metrics
    database.SchemaField("total_bot_requests", "INTEGER", mode="REQUIRED"),
    database.SchemaField("unique_bot_providers", "INTEGER", mode="REQUIRED"),
    database.SchemaField("unique_bot_names", "INTEGER", mode="REQUIRED"),
    # Category breakdown
    database.SchemaField("training_hits", "INTEGER", mode="REQUIRED"),
    database.SchemaField("user_request_hits", "INTEGER", mode="REQUIRED"),
    # Response breakdown
    database.SchemaField("successful_requests", "INTEGER", mode="REQUIRED"),
    database.SchemaField("error_requests", "INTEGER", mode="REQUIRED"),
    # Time tracking
    database.SchemaField("first_seen", "TIMESTAMP", mode="REQUIRED"),
    database.SchemaField("last_seen", "TIMESTAMP", mode="REQUIRED"),
    # Metadata
    database.SchemaField("_aggregated_at", "TIMESTAMP", mode="REQUIRED"),
]

URL_PERFORMANCE_PARTITION_FIELD = "request_date"
URL_PERFORMANCE_CLUSTERING_FIELDS = ["request_host", "url_path"]


# Data Freshness Table
# Tracks when data was last processed for monitoring
DATA_FRESHNESS_SCHEMA = [
    database.SchemaField("table_name", "STRING", mode="REQUIRED"),
    database.SchemaField("last_processed_date", "DATE", mode="REQUIRED"),
    database.SchemaField("last_updated_at", "TIMESTAMP", mode="REQUIRED"),
    database.SchemaField("rows_processed", "INTEGER", mode="REQUIRED"),
]

