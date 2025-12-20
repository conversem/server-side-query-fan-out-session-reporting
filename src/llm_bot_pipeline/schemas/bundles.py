"""
Query fan-out sessions schema for bundled LLM bot requests.

This schema defines the structure for storing query fan-out sessions,
which group temporally-clustered requests that likely originated from
a single user question to an LLM.
"""

from google.cloud import database

# =============================================================================
# Query Fan-Out Sessions Schema
# =============================================================================

QUERY_FANOUT_SESSIONS_SCHEMA = [
    # Session identification
    database.SchemaField("session_id", "STRING", mode="REQUIRED"),
    database.SchemaField("session_date", "DATE", mode="REQUIRED"),
    # Temporal bounds
    database.SchemaField("session_start_time", "TIMESTAMP", mode="REQUIRED"),
    database.SchemaField("session_end_time", "TIMESTAMP", mode="REQUIRED"),
    database.SchemaField("duration_ms", "INTEGER", mode="REQUIRED"),
    # Bot identification
    database.SchemaField("bot_provider", "STRING", mode="REQUIRED"),
    database.SchemaField("bot_name", "STRING", mode="NULLABLE"),
    # Request metrics
    database.SchemaField("request_count", "INTEGER", mode="REQUIRED"),
    database.SchemaField("unique_urls", "INTEGER", mode="REQUIRED"),
    # Semantic coherence metrics
    database.SchemaField("mean_cosine_similarity", "FLOAT64", mode="NULLABLE"),
    database.SchemaField("min_cosine_similarity", "FLOAT64", mode="NULLABLE"),
    database.SchemaField("max_cosine_similarity", "FLOAT64", mode="NULLABLE"),
    # Confidence classification
    database.SchemaField(
        "confidence_level", "STRING", mode="REQUIRED"
    ),  # 'high', 'medium', 'low'
    # Session naming (derived from first URL)
    database.SchemaField("fanout_session_name", "STRING", mode="NULLABLE"),
    # URL data (JSON array)
    database.SchemaField("url_list", "STRING", mode="REQUIRED"),  # JSON array
    # Configuration used
    database.SchemaField("window_ms", "FLOAT64", mode="REQUIRED"),
    # Metadata
    database.SchemaField("_created_at", "TIMESTAMP", mode="REQUIRED"),
]

# Partitioning and clustering configuration
QUERY_FANOUT_SESSIONS_PARTITION_FIELD = "session_date"
QUERY_FANOUT_SESSIONS_CLUSTERING_FIELDS = ["bot_provider", "confidence_level"]


# =============================================================================
# SQLite Schema (for local development)
# =============================================================================

QUERY_FANOUT_SESSIONS_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_fanout_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,
    session_date TEXT NOT NULL,
    session_start_time TEXT NOT NULL,
    session_end_time TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    bot_provider TEXT NOT NULL,
    bot_name TEXT,
    request_count INTEGER NOT NULL,
    unique_urls INTEGER NOT NULL,
    mean_cosine_similarity REAL,
    min_cosine_similarity REAL,
    max_cosine_similarity REAL,
    confidence_level TEXT NOT NULL,
    fanout_session_name TEXT,
    url_list TEXT NOT NULL,
    window_ms REAL NOT NULL,
    _created_at TEXT NOT NULL DEFAULT (datetime('now')),
    CONSTRAINT valid_confidence CHECK (confidence_level IN ('high', 'medium', 'low'))
)
"""

# Index definitions for query fan-out sessions
QUERY_FANOUT_SESSIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_sessions_date ON query_fanout_sessions(session_date)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_provider ON query_fanout_sessions(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_confidence ON query_fanout_sessions(confidence_level)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_request_count ON query_fanout_sessions(request_count)",
]


# =============================================================================
# Confidence Level Definitions
# =============================================================================

CONFIDENCE_THRESHOLDS = {
    "high": {"mean_similarity": 0.7, "min_similarity": 0.5},
    "medium": {"mean_similarity": 0.5, "min_similarity": 0.3},
    "low": {"mean_similarity": 0.0, "min_similarity": 0.0},
}


def get_confidence_level(mean_similarity: float, min_similarity: float) -> str:
    """
    Determine confidence level based on similarity scores.

    Args:
        mean_similarity: Mean pairwise cosine similarity
        min_similarity: Minimum pairwise cosine similarity

    Returns:
        Confidence level: 'high', 'medium', or 'low'
    """
    if (
        mean_similarity >= CONFIDENCE_THRESHOLDS["high"]["mean_similarity"]
        and min_similarity >= CONFIDENCE_THRESHOLDS["high"]["min_similarity"]
    ):
        return "high"
    elif (
        mean_similarity >= CONFIDENCE_THRESHOLDS["medium"]["mean_similarity"]
        and min_similarity >= CONFIDENCE_THRESHOLDS["medium"]["min_similarity"]
    ):
        return "medium"
    else:
        return "low"
