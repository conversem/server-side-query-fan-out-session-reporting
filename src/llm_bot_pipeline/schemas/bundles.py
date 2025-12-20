"""
Query fan-out sessions schema for bundled LLM bot requests.

This schema defines the structure for storing query fan-out sessions,
which group temporally-clustered requests that likely originated from
a single user question to an LLM.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# =============================================================================
# Query Fan-Out Sessions Schema (SQLite)
# =============================================================================

QUERY_FANOUT_SESSIONS_COLUMNS = {
    # Session identification
    "session_id": "TEXT PRIMARY KEY",
    "session_date": "DATE NOT NULL",
    # Temporal bounds
    "session_start_time": "TIMESTAMP NOT NULL",
    "session_end_time": "TIMESTAMP NOT NULL",
    "duration_ms": "INTEGER NOT NULL",
    # Bot identification
    "bot_provider": "TEXT NOT NULL",
    "bot_name": "TEXT",
    # Request metrics
    "request_count": "INTEGER NOT NULL",
    "unique_urls": "INTEGER NOT NULL",
    # Semantic coherence metrics
    "mean_cosine_similarity": "REAL",
    "min_cosine_similarity": "REAL",
    "max_cosine_similarity": "REAL",
    "thematic_variance": "REAL",
    # Classification
    "confidence_level": "TEXT",
    # Human-readable
    "fanout_session_name": "TEXT",
    # Data storage
    "url_list": "TEXT",  # JSON array
    # Metadata
    "_created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

# Index definitions for query fan-out sessions
QUERY_FANOUT_SESSIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_sessions_date ON query_fanout_sessions(session_date)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_provider ON query_fanout_sessions(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_confidence ON query_fanout_sessions(confidence_level)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_request_count ON query_fanout_sessions(request_count)",
]


@dataclass
class QueryFanoutSession:
    """Represents a query fan-out session."""

    session_id: str
    session_date: str
    session_start_time: datetime
    session_end_time: datetime
    duration_ms: int
    bot_provider: str
    request_count: int
    unique_urls: int
    bot_name: Optional[str] = None
    mean_cosine_similarity: Optional[float] = None
    min_cosine_similarity: Optional[float] = None
    max_cosine_similarity: Optional[float] = None
    thematic_variance: Optional[float] = None
    confidence_level: Optional[str] = None
    fanout_session_name: Optional[str] = None
    url_list: Optional[str] = None


def get_create_sessions_table_sql() -> str:
    """Get SQL to create the query_fanout_sessions table."""
    columns = ", ".join(
        f"{name} {dtype}" for name, dtype in QUERY_FANOUT_SESSIONS_COLUMNS.items()
    )
    return f"CREATE TABLE IF NOT EXISTS query_fanout_sessions ({columns})"


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
