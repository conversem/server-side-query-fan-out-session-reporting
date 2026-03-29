"""
BigQuery schema definitions for the LLM bot traffic pipeline.

Contains all table SchemaField definitions and TABLE_CONFIG (dataset routing,
partitioning, clustering). Separated from bigquery_backend.py to keep schema
constants distinct from client operations.

Mirrors the pattern used in sqlite_schemas.py for SQLite DDL.
"""

from typing import Any

# =============================================================================
# Table Schemas (lazy-loaded to avoid requiring google-cloud-bigquery at import)
# =============================================================================


def _get_table_schemas() -> dict[str, list]:
    """Return {table_name: [SchemaField, ...]} for all pipeline tables.

    Lazy-imports google.cloud.bigquery so callers that only need TABLE_CONFIG
    don't pull in the SDK.
    """
    from google.cloud.bigquery import SchemaField

    S = SchemaField
    R = "REQUIRED"
    N = "NULLABLE"
    return {
        "raw_bot_requests": [
            S("EdgeStartTimestamp", "INT64", mode=R),
            S("ClientRequestURI", "STRING", mode=N),
            S("ClientRequestHost", "STRING", mode=N),
            S("domain", "STRING", mode=N),
            S("ClientRequestUserAgent", "STRING", mode=N),
            S("ClientIP", "STRING", mode=N),
            S("ClientCountry", "STRING", mode=N),
            S("EdgeResponseStatus", "INTEGER", mode=N),
            S("RayID", "STRING", mode=N),
            S("_bot_name", "STRING", mode=N),
            S("_bot_provider", "STRING", mode=N),
            S("_bot_category", "STRING", mode=N),
            S("_ingestion_time", "TIMESTAMP", mode=N),
            S("source_provider", "STRING", mode=N),
        ],
        "bot_requests_daily": [
            S("request_timestamp", "TIMESTAMP", mode=R),
            S("request_date", "DATE", mode=R),
            S("request_hour", "INTEGER", mode=R),
            S("day_of_week", "STRING", mode=R),
            S("request_uri", "STRING", mode=R),
            S("request_host", "STRING", mode=R),
            S("domain", "STRING", mode=N),
            S("url_path", "STRING", mode=N),
            S("url_path_depth", "INTEGER", mode=N),
            S("user_agent_raw", "STRING", mode=N),
            S("bot_name", "STRING", mode=R),
            S("bot_provider", "STRING", mode=R),
            S("bot_category", "STRING", mode=R),
            S("crawler_country", "STRING", mode=N),
            S("response_status", "INTEGER", mode=R),
            S("response_status_category", "STRING", mode=R),
            S("resource_type", "STRING", mode=R),
            S("_processed_at", "TIMESTAMP", mode=R),
        ],
        "daily_summary": [
            S("request_date", "DATE", mode=R),
            S("domain", "STRING", mode=N),
            S("bot_provider", "STRING", mode=R),
            S("bot_name", "STRING", mode=R),
            S("bot_category", "STRING", mode=R),
            S("total_requests", "INTEGER", mode=R),
            S("unique_urls", "INTEGER", mode=R),
            S("unique_hosts", "INTEGER", mode=R),
            S("successful_requests", "INTEGER", mode=R),
            S("error_requests", "INTEGER", mode=R),
            S("redirect_requests", "INTEGER", mode=R),
            S("_aggregated_at", "TIMESTAMP", mode=R),
        ],
        "url_performance": [
            S("request_date", "DATE", mode=R),
            S("domain", "STRING", mode=N),
            S("request_host", "STRING", mode=R),
            S("url_path", "STRING", mode=R),
            S("total_bot_requests", "INTEGER", mode=R),
            S("unique_bot_providers", "INTEGER", mode=R),
            S("unique_bot_names", "INTEGER", mode=R),
            S("training_hits", "INTEGER", mode=R),
            S("user_request_hits", "INTEGER", mode=R),
            S("successful_requests", "INTEGER", mode=R),
            S("error_requests", "INTEGER", mode=R),
            S("first_seen", "TIMESTAMP", mode=R),
            S("last_seen", "TIMESTAMP", mode=R),
            S("_aggregated_at", "TIMESTAMP", mode=R),
        ],
        "data_freshness": [
            S("table_name", "STRING", mode=R),
            S("last_processed_date", "DATE", mode=R),
            S("last_updated_at", "TIMESTAMP", mode=R),
            S("rows_processed", "INTEGER", mode=R),
        ],
        "query_fanout_sessions": [
            S("session_id", "STRING", mode=R),
            S("session_date", "DATE", mode=R),
            S("domain", "STRING", mode=N),
            S("session_start_time", "TIMESTAMP", mode=R),
            S("session_end_time", "TIMESTAMP", mode=R),
            S("duration_ms", "INTEGER", mode=R),
            S("bot_provider", "STRING", mode=R),
            S("bot_name", "STRING", mode=N),
            S("request_count", "INTEGER", mode=R),
            S("unique_urls", "INTEGER", mode=R),
            S("mean_cosine_similarity", "FLOAT64", mode=N),
            S("min_cosine_similarity", "FLOAT64", mode=N),
            S("max_cosine_similarity", "FLOAT64", mode=N),
            S("confidence_level", "STRING", mode=R),
            S("fanout_session_name", "STRING", mode=N),
            S("url_list", "STRING", mode=R),
            S("window_ms", "FLOAT64", mode=R),
            S("splitting_strategy", "STRING", mode=N),
            S("_created_at", "TIMESTAMP", mode=R),
            S("parent_session_id", "STRING", mode=N),
            S("was_refined", "BOOLEAN", mode=N),
            S("refinement_reason", "STRING", mode=N),
            S("pre_refinement_mibcs", "FLOAT64", mode=N),
        ],
        "session_url_details": [
            S("session_id", "STRING", mode=R),
            S("session_date", "DATE", mode=R),
            S("domain", "STRING", mode=N),
            S("url", "STRING", mode=R),
            S("url_position", "INTEGER", mode=R),
            S("bot_provider", "STRING", mode=R),
            S("bot_name", "STRING", mode=N),
            S("fanout_session_name", "STRING", mode=N),
            S("confidence_level", "STRING", mode=R),
            S("session_request_count", "INTEGER", mode=R),
            S("session_unique_urls", "INTEGER", mode=R),
            S("session_duration_ms", "INTEGER", mode=R),
            S("mean_cosine_similarity", "FLOAT64", mode=N),
            S("min_cosine_similarity", "FLOAT64", mode=N),
            S("max_cosine_similarity", "FLOAT64", mode=N),
            S("session_start_time", "TIMESTAMP", mode=R),
            S("session_end_time", "TIMESTAMP", mode=R),
            S("window_ms", "FLOAT64", mode=R),
            S("splitting_strategy", "STRING", mode=N),
            S("_created_at", "TIMESTAMP", mode=R),
        ],
        "sitemap_urls": [
            S("url", "STRING", mode=R),
            S("url_path", "STRING", mode=R),
            S("domain", "STRING", mode=N),
            S("lastmod", "STRING", mode=N),
            S("lastmod_month", "STRING", mode=N),
            S("sitemap_source", "STRING", mode=R),
            S("_fetched_at", "TIMESTAMP", mode=R),
        ],
        "sitemap_freshness": [
            S("url_path", "STRING", mode=R),
            S("domain", "STRING", mode=N),
            S("lastmod", "STRING", mode=N),
            S("lastmod_month", "STRING", mode=N),
            S("sitemap_source", "STRING", mode=R),
            S("first_seen_date", "DATE", mode=N),
            S("last_seen_date", "DATE", mode=N),
            S("request_count", "INTEGER", mode=R),
            S("unique_urls", "INTEGER", mode=R),
            S("unique_bots", "INTEGER", mode=R),
            S("days_since_lastmod", "INTEGER", mode=N),
            S("_aggregated_at", "TIMESTAMP", mode=R),
        ],
        "url_volume_decay": [
            S("url_path", "STRING", mode=R),
            S("domain", "STRING", mode=N),
            S("period", "STRING", mode=R),
            S("period_start", "DATE", mode=R),
            S("request_count", "INTEGER", mode=R),
            S("unique_urls", "INTEGER", mode=R),
            S("unique_bots", "INTEGER", mode=R),
            S("prev_request_count", "INTEGER", mode=N),
            S("decay_rate", "FLOAT64", mode=N),
            S("_aggregated_at", "TIMESTAMP", mode=R),
        ],
        "session_refinement_log": [
            S("run_timestamp", "TIMESTAMP", mode=N),
            S("window_ms", "FLOAT64", mode=R),
            S("total_bundles", "INTEGER", mode=R),
            S("collision_candidates", "INTEGER", mode=R),
            S("bundles_split", "INTEGER", mode=R),
            S("sub_bundles_created", "INTEGER", mode=R),
            S("mean_mibcs_improvement", "FLOAT64", mode=N),
            S("refinement_duration_ms", "FLOAT64", mode=N),
            S("collision_ip_threshold", "INTEGER", mode=N),
            S("collision_homogeneity_threshold", "FLOAT64", mode=N),
            S("similarity_threshold", "FLOAT64", mode=N),
            S("min_sub_bundle_size", "INTEGER", mode=N),
            S("min_mibcs_improvement", "FLOAT64", mode=N),
        ],
    }


# =============================================================================
# Table Configuration (dataset routing, partitioning, clustering)
# =============================================================================

# dataset_key: "raw" or "report" — resolved at runtime by BigQueryBackend
TABLE_CONFIG: dict[str, dict[str, Any]] = {
    "raw_bot_requests": {
        "dataset_key": "raw",
        "partition_field": None,
        "clustering_fields": None,
    },
    "bot_requests_daily": {
        "dataset_key": "report",
        "partition_field": "request_date",
        "clustering_fields": ["domain", "bot_provider", "bot_category"],
    },
    "daily_summary": {
        "dataset_key": "report",
        "partition_field": "request_date",
        "clustering_fields": ["domain", "bot_provider", "bot_category"],
    },
    "url_performance": {
        "dataset_key": "report",
        "partition_field": "request_date",
        "clustering_fields": ["domain", "request_host", "url_path"],
    },
    "data_freshness": {
        "dataset_key": "report",
        "partition_field": None,
        "clustering_fields": None,
    },
    "query_fanout_sessions": {
        "dataset_key": "report",
        "partition_field": "session_date",
        "clustering_fields": ["domain", "bot_provider", "confidence_level"],
    },
    "session_url_details": {
        "dataset_key": "report",
        "partition_field": "session_date",
        "clustering_fields": ["domain", "bot_provider", "url"],
    },
    "sitemap_urls": {
        "dataset_key": "report",
        "partition_field": None,
        "clustering_fields": ["domain", "sitemap_source"],
    },
    "sitemap_freshness": {
        "dataset_key": "report",
        "partition_field": None,
        "clustering_fields": ["domain", "sitemap_source", "lastmod_month"],
    },
    "url_volume_decay": {
        "dataset_key": "report",
        "partition_field": "period_start",
        "clustering_fields": ["domain", "period", "url_path"],
    },
    "session_refinement_log": {
        "dataset_key": "report",
        "partition_field": None,
        "clustering_fields": None,
    },
}
