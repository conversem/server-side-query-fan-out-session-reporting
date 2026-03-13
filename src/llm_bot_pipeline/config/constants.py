"""
Constants for LLM bot classification, table names, and pipeline defaults.
"""

# =============================================================================
# Query Fan-Out Session Configuration
# =============================================================================

# Time window options for session grouping (validated via research)
# See docs/research/ for validation details
#
# Timing analysis of user_request records shows:
#   - Mode: 9ms, Median: 10ms, P75: 15ms, P90: 53ms
#   - 84% of burst gaps are ≤20ms
#
# Window comparison (high confidence %):
#   - 50ms: 94.6% high confidence, captures 97% of burst gaps
#   - 100ms: 93.9% high confidence, captures 91% of burst gaps
#
WINDOW_50MS = 50  # Tighter grouping, higher coherence
WINDOW_100MS = 100  # Conservative, well-tested default

OPTIMAL_WINDOW_MS = WINDOW_100MS

# =============================================================================
# Bot Classification
# =============================================================================

# Bot categories:
# - training: Crawlers collecting data for AI model training
#     (excluded from session analysis)
# - user_request: Real-time browsing triggered by user queries
#     (included in session analysis)
# - search: Search indexing bots (AI-powered and traditional)
#     (excluded from session analysis)
#
# See docs/ai-bot-user-agents.md for complete reference and official links.

BOT_CLASSIFICATION = {
    # OpenAI - https://platform.openai.com/docs/bots
    "GPTBot": {"provider": "OpenAI", "category": "training"},
    "ChatGPT-User": {"provider": "OpenAI", "category": "user_request"},
    "OAI-SearchBot": {"provider": "OpenAI", "category": "search"},
    # Anthropic - https://www.anthropic.com
    "ClaudeBot": {"provider": "Anthropic", "category": "training"},
    "Claude-User": {"provider": "Anthropic", "category": "user_request"},
    "Claude-SearchBot": {"provider": "Anthropic", "category": "search"},
    # Google - https://developers.google.com/search/docs/crawling-indexing/overview-google-crawlers
    "Google-Extended": {"provider": "Google", "category": "training"},
    "Gemini-Deep-Research": {"provider": "Google", "category": "user_request"},
    "Google-CloudVertexBot": {"provider": "Google", "category": "user_request"},
    "Googlebot": {"provider": "Google", "category": "search"},
    # Perplexity - https://docs.perplexity.ai/guides/bots
    "PerplexityBot": {"provider": "Perplexity", "category": "search"},
    "Perplexity-User": {"provider": "Perplexity", "category": "user_request"},
    # Apple - https://support.apple.com/en-us/HT204683
    "Applebot-Extended": {"provider": "Apple", "category": "training"},
    # Microsoft - https://www.bing.com/webmaster/help/which-crawlers-does-bing-use-8c184ec0
    "bingbot": {"provider": "Microsoft", "category": "search"},
    # Meta - https://developers.facebook.com/docs/sharing/webmasters/crawler
    "Meta-ExternalAgent": {"provider": "Meta", "category": "training"},
    "Meta-WebIndexer": {"provider": "Meta", "category": "search"},
    # Mistral - https://docs.mistral.ai/robots
    "MistralAI-User": {"provider": "Mistral", "category": "user_request"},
    # Amazon
    "Amazonbot": {"provider": "Amazon", "category": "training"},
    # DuckDuckGo
    "DuckAssistBot": {"provider": "DuckDuckGo", "category": "search"},
    # ByteDance
    "Bytespider": {"provider": "ByteDance", "category": "training"},
    # Common Crawl (used by many AI companies for training)
    "CCBot": {"provider": "CommonCrawl", "category": "training"},
    # Diffbot - Data extraction for AI companies
    "Diffbot": {"provider": "Diffbot", "category": "training"},
}

LLM_BOT_NAMES = list(BOT_CLASSIFICATION.keys())

# =============================================================================
# Cloudflare-Specific Fields
# =============================================================================

# Standard Logpull API fields (works with any Cloudflare plan)
OUTPUT_FIELDS = [
    "EdgeStartTimestamp",
    "ClientRequestURI",
    "ClientRequestHost",
    "ClientRequestUserAgent",
    "ClientIP",
    "ClientCountry",
    "EdgeResponseStatus",
    "RayID",
]

# Full field set for Logpush (requires Enterprise plan with Bot Management add-on)
LOGPUSH_FIELDS = OUTPUT_FIELDS + [
    "BotScore",
    "BotScoreSrc",
    "VerifiedBot",
    "BotTags",
]

# =============================================================================
# Table Names (shared across backends)
# =============================================================================

TABLE_RAW_BOT_REQUESTS = "raw_bot_requests"
TABLE_CLEAN_BOT_REQUESTS = "bot_requests_daily"
TABLE_DAILY_SUMMARY = "daily_summary"
TABLE_URL_PERFORMANCE = "url_performance"
TABLE_DATA_FRESHNESS = "data_freshness"
TABLE_QUERY_FANOUT_SESSIONS = "query_fanout_sessions"
TABLE_SESSION_URL_DETAILS = "session_url_details"
TABLE_SESSION_REFINEMENT_LOG = "session_refinement_log"
TABLE_SITEMAP_URLS = "sitemap_urls"
TABLE_SITEMAP_FRESHNESS = "sitemap_freshness"
TABLE_URL_VOLUME_DECAY = "url_volume_decay"

VALID_TABLE_NAMES = frozenset(
    [
        TABLE_RAW_BOT_REQUESTS,
        TABLE_CLEAN_BOT_REQUESTS,
        TABLE_DAILY_SUMMARY,
        TABLE_URL_PERFORMANCE,
        TABLE_DATA_FRESHNESS,
        TABLE_QUERY_FANOUT_SESSIONS,
        TABLE_SESSION_URL_DETAILS,
        TABLE_SESSION_REFINEMENT_LOG,
        TABLE_SITEMAP_URLS,
        TABLE_SITEMAP_FRESHNESS,
        TABLE_URL_VOLUME_DECAY,
    ]
)

# =============================================================================
# BigQuery View Names (reporting views created alongside tables)
# =============================================================================

VIEW_DAILY_KPIS = "v_daily_kpis"
VIEW_SESSION_URL_DISTRIBUTION = "v_session_url_distribution"
VIEW_SESSION_SINGLETON_BINARY = "v_session_singleton_binary"
VIEW_BOT_VOLUME = "v_bot_volume"
VIEW_TOP_SESSION_TOPICS = "v_top_session_topics"
VIEW_CATEGORY_COMPARISON = "v_category_comparison"
VIEW_URL_COOCCURRENCE = "v_url_cooccurrence"
VIEW_URL_FRESHNESS = "v_url_freshness"
VIEW_DECAY_UNIQUE_URLS = "v_decay_unique_urls"
VIEW_DECAY_REQUEST_VOLUME = "v_decay_request_volume"

BIGQUERY_VIEW_NAMES = frozenset(
    [
        VIEW_DAILY_KPIS,
        VIEW_SESSION_URL_DISTRIBUTION,
        VIEW_SESSION_SINGLETON_BINARY,
        VIEW_BOT_VOLUME,
        VIEW_TOP_SESSION_TOPICS,
        VIEW_CATEGORY_COMPARISON,
        VIEW_URL_COOCCURRENCE,
        VIEW_URL_FRESHNESS,
        VIEW_DECAY_UNIQUE_URLS,
        VIEW_DECAY_REQUEST_VOLUME,
    ]
)

# =============================================================================
# Date Column Whitelist (SQL-injection prevention for date filtering)
# =============================================================================

VALID_DATE_COLUMNS = frozenset(
    [
        "EdgeStartTimestamp",
        "request_date",
        "request_timestamp",
        "session_date",
        "session_start_time",
        "session_end_time",
        "_processed_at",
        "_created_at",
        "_aggregated_at",
        "_fetched_at",
        "first_seen",
        "last_seen",
        "first_seen_date",
        "last_seen_date",
        "period_start",
    ]
)

# Order-by column whitelist (SQL-injection prevention for read_records order_by)
VALID_ORDER_BY_COLUMNS = VALID_DATE_COLUMNS | frozenset(
    [
        "id",
        "url_path",
        "url",
        "request_uri",
        "request_host",
        "bot_name",
        "bot_provider",
        "bot_category",
        "total_requests",
        "unique_urls",
        "unique_hosts",
        "request_hour",
        "day_of_week",
        "lastmod",
        "lastmod_month",
        "sitemap_source",
        "table_name",
        "last_processed_date",
        "last_updated_at",
        "rows_processed",
        "session_id",
        "duration_ms",
        "request_count",
        "period",
    ]
)

# =============================================================================
# Dataset Names (BigQuery)
# =============================================================================

DATASET_RAW = "bot_logs_raw"
DATASET_STAGING = "bot_logs_staging"
DATASET_REPORT = "bot_logs"

# =============================================================================
# Pipeline Configuration Defaults (BigQuery service account)
# =============================================================================

SERVICE_ACCOUNT_ID = "llm-bot-pipeline"
SERVICE_ACCOUNT_DISPLAY_NAME = "LLM Bot Pipeline BigQuery"
KEY_FILE_NAME = "credentials/bigquery_key.json"

# =============================================================================
# Processing Modes
# =============================================================================

VALID_PROCESSING_MODES = (
    "local_sqlite",
    "local_bq_buffered",
    "local_bq_streaming",
    "gcp_bq",
)

# Modes that require BigQuery configuration
BQ_PROCESSING_MODES = ("local_bq_buffered", "local_bq_streaming", "gcp_bq")

# Modes that require SQLite
SQLITE_PROCESSING_MODES = ("local_sqlite", "local_bq_buffered")

# =============================================================================
# Streaming Pipeline Defaults
# =============================================================================

DEFAULT_STREAMING_BATCH_SIZE = 1000
DEFAULT_MAX_PENDING_BATCHES = 5

# =============================================================================
# Cloud Run Defaults
# =============================================================================

DEFAULT_CLOUD_RUN_JOB_NAME = "llm-bot-daily-etl"
DEFAULT_SCHEDULER_NAME = "daily-etl-trigger"
DEFAULT_SCHEDULE = "0 4 * * *"
DEFAULT_TIMEZONE = "Europe/Brussels"
DEFAULT_CLOUD_RUN_MEMORY = "2Gi"
DEFAULT_CLOUD_RUN_CPU = "1"
DEFAULT_CLOUD_RUN_TIMEOUT_SECONDS = 3600

# =============================================================================
# BigQuery Batch Sizes
# =============================================================================

# Threshold below which streaming insert is used; above it a load job is preferred
BATCH_THRESHOLD = 1000
# Max rows per streaming-insert call for session records
BIGQUERY_BATCH_SIZE = 10000

# =============================================================================
# Excel Export Defaults
# =============================================================================

AUTOFIT_SAMPLE_ROWS = 100
EXCEL_DATETIME_FORMAT = "YYYY-MM-DD HH:MM:SS"
EXCEL_DATE_FORMAT = "YYYY-MM-DD"
