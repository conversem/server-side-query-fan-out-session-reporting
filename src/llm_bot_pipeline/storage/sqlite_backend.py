"""
SQLite storage backend implementation.

Provides a local SQLite-based storage for local development,
with schema matching the SQLite production structure.
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from .base import QueryError, SchemaError, StorageBackend, StorageConnectionError

logger = logging.getLogger(__name__)


# =============================================================================
# SQLite Schema Definitions
# =============================================================================

RAW_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_bot_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    EdgeStartTimestamp TEXT NOT NULL,
    ClientRequestURI TEXT,
    ClientRequestHost TEXT,
    ClientRequestUserAgent TEXT,
    BotScore INTEGER,
    BotScoreSrc TEXT,
    VerifiedBot INTEGER,
    BotTags TEXT,  -- JSON array stored as string
    ClientIP TEXT,
    ClientCountry TEXT,
    EdgeResponseStatus INTEGER,
    _ingestion_time TEXT NOT NULL,
    source_provider TEXT  -- Tracks data provenance (universal, cloudflare, aws_cloudfront)
)
"""

CLEAN_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_requests_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_timestamp TEXT NOT NULL,
    request_date TEXT NOT NULL,
    request_hour INTEGER NOT NULL,
    day_of_week TEXT NOT NULL,
    request_uri TEXT NOT NULL,
    request_host TEXT NOT NULL,
    url_path TEXT,
    url_path_depth INTEGER,
    user_agent_raw TEXT,
    bot_name TEXT NOT NULL,
    bot_provider TEXT NOT NULL,
    bot_category TEXT NOT NULL,
    bot_score INTEGER,
    is_verified_bot INTEGER NOT NULL,
    crawler_country TEXT,
    response_status INTEGER NOT NULL,
    response_status_category TEXT NOT NULL,
    _processed_at TEXT NOT NULL
)
"""

DAILY_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_date TEXT NOT NULL,
    bot_provider TEXT NOT NULL,
    bot_name TEXT NOT NULL,
    bot_category TEXT NOT NULL,
    total_requests INTEGER NOT NULL,
    unique_urls INTEGER NOT NULL,
    unique_hosts INTEGER NOT NULL,
    avg_bot_score REAL,
    successful_requests INTEGER NOT NULL,
    error_requests INTEGER NOT NULL,
    redirect_requests INTEGER NOT NULL,
    _aggregated_at TEXT NOT NULL
)
"""

URL_PERFORMANCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS url_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_date TEXT NOT NULL,
    request_host TEXT NOT NULL,
    url_path TEXT NOT NULL,
    total_bot_requests INTEGER NOT NULL,
    unique_bot_providers INTEGER NOT NULL,
    unique_bot_names INTEGER NOT NULL,
    training_hits INTEGER NOT NULL,
    user_request_hits INTEGER NOT NULL,
    successful_requests INTEGER NOT NULL,
    error_requests INTEGER NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    _aggregated_at TEXT NOT NULL
)
"""

DATA_FRESHNESS_SCHEMA = """
CREATE TABLE IF NOT EXISTS data_freshness (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    last_processed_date TEXT NOT NULL,
    last_updated_at TEXT NOT NULL,
    rows_processed INTEGER NOT NULL
)
"""

QUERY_FANOUT_SESSIONS_SCHEMA = """
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

# Index definitions for query performance
INDEX_DEFINITIONS = [
    # Raw table indexes
    "CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_bot_requests(EdgeStartTimestamp)",
    "CREATE INDEX IF NOT EXISTS idx_raw_host ON raw_bot_requests(ClientRequestHost)",
    # Clean table indexes (matching SQLite clustering)
    "CREATE INDEX IF NOT EXISTS idx_clean_date ON bot_requests_daily(request_date)",
    "CREATE INDEX IF NOT EXISTS idx_clean_provider ON bot_requests_daily(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_clean_category ON bot_requests_daily(bot_category)",
    "CREATE INDEX IF NOT EXISTS idx_clean_host ON bot_requests_daily(request_host)",
    # Summary table indexes
    "CREATE INDEX IF NOT EXISTS idx_summary_date ON daily_summary(request_date)",
    "CREATE INDEX IF NOT EXISTS idx_summary_provider ON daily_summary(bot_provider)",
    # URL performance indexes
    "CREATE INDEX IF NOT EXISTS idx_url_date ON url_performance(request_date)",
    "CREATE INDEX IF NOT EXISTS idx_url_host ON url_performance(request_host)",
    # Query fan-out sessions indexes
    "CREATE INDEX IF NOT EXISTS idx_sessions_date ON query_fanout_sessions(session_date)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_provider ON query_fanout_sessions(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_confidence ON query_fanout_sessions(confidence_level)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_request_count ON query_fanout_sessions(request_count)",
]


# =============================================================================
# Type Conversion Helpers
# =============================================================================


def _to_sqlite_timestamp(value: Any) -> Optional[str]:
    """Convert datetime/timestamp to ISO8601 string for SQLite.

    Handles:
    - datetime objects
    - ISO8601 strings
    - Cloudflare nanosecond timestamps (integers > 1e15)
    - Unix timestamps (integers/floats)
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        # Check if it's a numeric string (nanoseconds)
        try:
            numeric_val = int(value)
            if numeric_val > 1e15:  # Nanoseconds since epoch
                return datetime.fromtimestamp(numeric_val / 1e9).isoformat()
            elif numeric_val > 1e9:  # Milliseconds since epoch
                return datetime.fromtimestamp(numeric_val / 1e3).isoformat()
            else:  # Seconds since epoch
                return datetime.fromtimestamp(numeric_val).isoformat()
        except (ValueError, TypeError):
            return value  # Assume already ISO format
    if isinstance(value, (int, float)):
        # Handle numeric timestamps
        if value > 1e15:  # Nanoseconds since epoch (Cloudflare format)
            return datetime.fromtimestamp(value / 1e9).isoformat()
        elif value > 1e12:  # Milliseconds since epoch
            return datetime.fromtimestamp(value / 1e3).isoformat()
        else:  # Seconds since epoch
            return datetime.fromtimestamp(value).isoformat()
    return str(value)


def _to_sqlite_date(value: Any) -> Optional[str]:
    """Convert date to YYYY-MM-DD string for SQLite."""
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        return value  # Assume already in correct format
    return str(value)


def _to_sqlite_bool(value: Any) -> Optional[int]:
    """Convert boolean to INTEGER (0/1) for SQLite."""
    if value is None:
        return None
    return 1 if value else 0


def _to_sqlite_json(value: Any) -> Optional[str]:
    """Convert list/dict to JSON string for SQLite."""
    if value is None:
        return None
    if isinstance(value, str):
        return value  # Assume already JSON
    return json.dumps(value)


# The following functions convert SQLite values back to Python types.
# They are exported for use by pipeline code when reading query results.


def from_sqlite_bool(value: Any) -> Optional[bool]:
    """Convert SQLite INTEGER to Python bool."""
    if value is None:
        return None
    return bool(value)


def from_sqlite_json(value: Any) -> Optional[list | dict]:
    """Convert JSON string to Python list/dict."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def from_sqlite_timestamp(value: Any) -> Optional[datetime]:
    """Convert ISO8601 string to datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# =============================================================================
# Validation Helpers
# =============================================================================

# Valid table names in our schema
VALID_TABLES = frozenset(
    [
        "raw_bot_requests",
        "bot_requests_daily",
        "daily_summary",
        "url_performance",
        "data_freshness",
        "query_fanout_sessions",
    ]
)

# Valid column names for date filtering
VALID_DATE_COLUMNS = frozenset(
    [
        "EdgeStartTimestamp",
        "request_date",
        "request_timestamp",
        "last_processed_date",
        "first_seen",
        "last_seen",
        "_ingestion_time",
        "_processed_at",
        "_aggregated_at",
        "session_date",
        "session_start_time",
        "session_end_time",
        "_created_at",
    ]
)


def _validate_identifier(value: str, valid_set: frozenset, name: str) -> str:
    """
    Validate an identifier against a whitelist to prevent SQL injection.

    Args:
        value: The identifier to validate
        valid_set: Set of valid identifiers
        name: Human-readable name for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If identifier is not in the valid set
    """
    if value not in valid_set:
        raise ValueError(
            f"Invalid {name}: '{value}'. Must be one of: {sorted(valid_set)}"
        )
    return value


# =============================================================================
# SQLite Backend Implementation
# =============================================================================


class SQLiteBackend(StorageBackend):
    """
    SQLite storage backend for local development.

    Provides local storage with schema matching SQLite structure,
    enabling development and testing without cloud dependencies.
    """

    def __init__(
        self,
        db_path: Path | str = "data/llm-bot-logs.db",
        *,
        check_same_thread: bool = False,
        timeout: float = 30.0,
    ):
        """
        Initialize SQLite backend.

        Args:
            db_path: Path to SQLite database file
            check_same_thread: SQLite check_same_thread parameter
            timeout: Connection timeout in seconds
        """
        self.db_path = Path(db_path)
        self._check_same_thread = check_same_thread
        self._timeout = timeout
        self._connection: Optional[sqlite3.Connection] = None

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def backend_type(self) -> str:
        """Return backend type identifier."""
        return "sqlite"

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._connection is None:
            try:
                self._connection = sqlite3.connect(
                    str(self.db_path),
                    check_same_thread=self._check_same_thread,
                    timeout=self._timeout,
                )
                # Enable foreign keys and return rows as dictionaries
                self._connection.row_factory = sqlite3.Row
                self._connection.execute("PRAGMA foreign_keys = ON")
                logger.debug(f"Connected to SQLite database: {self.db_path}")
            except sqlite3.Error as e:
                raise StorageConnectionError(
                    f"Failed to connect to SQLite database: {e}"
                ) from e
        return self._connection

    @contextmanager
    def _cursor(self):
        """Context manager for database cursor with automatic commit/rollback."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            raise QueryError(f"SQLite query failed: {e}") from e
        finally:
            cursor.close()

    def initialize(self) -> None:
        """
        Initialize database with all required tables and indexes.

        Safe to call multiple times - uses IF NOT EXISTS.
        """
        logger.info(f"Initializing SQLite database: {self.db_path}")

        with self._cursor() as cursor:
            # Create tables
            cursor.execute(RAW_TABLE_SCHEMA)
            cursor.execute(CLEAN_TABLE_SCHEMA)
            cursor.execute(DAILY_SUMMARY_SCHEMA)
            cursor.execute(URL_PERFORMANCE_SCHEMA)
            cursor.execute(DATA_FRESHNESS_SCHEMA)
            cursor.execute(QUERY_FANOUT_SESSIONS_SCHEMA)

            # Create indexes
            for index_sql in INDEX_DEFINITIONS:
                cursor.execute(index_sql)

        logger.info("SQLite database initialized successfully")

    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug("SQLite connection closed")

    def insert_raw_records(self, records: list[dict]) -> int:
        """
        Insert raw log records into raw_bot_requests table.

        Args:
            records: List of raw log records from Cloudflare

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        sql = """
            INSERT INTO raw_bot_requests (
                EdgeStartTimestamp, ClientRequestURI, ClientRequestHost,
                ClientRequestUserAgent, BotScore, BotScoreSrc, VerifiedBot,
                BotTags, ClientIP, ClientCountry, EdgeResponseStatus,
                _ingestion_time, source_provider
            ) VALUES (
                :EdgeStartTimestamp, :ClientRequestURI, :ClientRequestHost,
                :ClientRequestUserAgent, :BotScore, :BotScoreSrc, :VerifiedBot,
                :BotTags, :ClientIP, :ClientCountry, :EdgeResponseStatus,
                :_ingestion_time, :source_provider
            )
        """

        # Convert records for SQLite
        converted_records = []
        now = datetime.now().astimezone().isoformat()

        for record in records:
            converted = {
                "EdgeStartTimestamp": _to_sqlite_timestamp(
                    record.get("EdgeStartTimestamp")
                ),
                "ClientRequestURI": record.get("ClientRequestURI"),
                "ClientRequestHost": record.get("ClientRequestHost"),
                "ClientRequestUserAgent": record.get("ClientRequestUserAgent"),
                "BotScore": record.get("BotScore"),
                "BotScoreSrc": record.get("BotScoreSrc"),
                "VerifiedBot": _to_sqlite_bool(record.get("VerifiedBot")),
                "BotTags": _to_sqlite_json(record.get("BotTags")),
                "ClientIP": record.get("ClientIP"),
                "ClientCountry": record.get("ClientCountry"),
                "EdgeResponseStatus": record.get("EdgeResponseStatus"),
                "_ingestion_time": record.get("_ingestion_time", now),
                "source_provider": record.get("source_provider"),
            }
            converted_records.append(converted)

        with self._cursor() as cursor:
            cursor.executemany(sql, converted_records)
            # executemany may not set rowcount correctly; use len instead
            return len(converted_records)

    def query(
        self,
        sql: str,
        params: Optional[dict] = None,
    ) -> list[dict]:
        """
        Execute query and return results as list of dictionaries.

        Args:
            sql: SQL query (use :param_name for parameters)
            params: Optional parameter dictionary

        Returns:
            List of result rows as dictionaries
        """
        with self._cursor() as cursor:
            cursor.execute(sql, params or {})
            columns = [desc[0] for desc in cursor.description or []]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def execute(
        self,
        sql: str,
        params: Optional[dict] = None,
    ) -> int:
        """
        Execute statement (INSERT, UPDATE, DELETE, DDL).

        Args:
            sql: SQL statement
            params: Optional parameter dictionary

        Returns:
            Number of affected rows
        """
        with self._cursor() as cursor:
            cursor.execute(sql, params or {})
            return cursor.rowcount

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        sql = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=:table_name
        """
        result = self.query(sql, {"table_name": table_name})
        return len(result) > 0

    def get_table_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")

        # Use f-string here - table_name is validated by table_exists
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.query(sql)
        return result[0]["count"] if result else 0

    def get_date_range_count(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> int:
        """Get row count for a specific date range."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")

        # Validate date_column to prevent SQL injection
        _validate_identifier(date_column, VALID_DATE_COLUMNS, "date column")

        # SQLite uses TEXT for dates, so comparison works with ISO format
        sql = f"""
            SELECT COUNT(*) as count
            FROM {table_name}
            WHERE {date_column} >= :start_date
              AND {date_column} <= :end_date
        """
        result = self.query(
            sql,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
        return result[0]["count"] if result else 0

    # =========================================================================
    # SQLite-specific helper methods
    # =========================================================================

    def insert_clean_records(self, records: list[dict]) -> int:
        """
        Insert processed records into bot_requests_daily table.

        Args:
            records: List of cleaned/processed records

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        sql = """
            INSERT INTO bot_requests_daily (
                request_timestamp, request_date, request_hour, day_of_week,
                request_uri, request_host, url_path, url_path_depth,
                user_agent_raw, bot_name, bot_provider, bot_category,
                bot_score, is_verified_bot, crawler_country,
                response_status, response_status_category, _processed_at
            ) VALUES (
                :request_timestamp, :request_date, :request_hour, :day_of_week,
                :request_uri, :request_host, :url_path, :url_path_depth,
                :user_agent_raw, :bot_name, :bot_provider, :bot_category,
                :bot_score, :is_verified_bot, :crawler_country,
                :response_status, :response_status_category, :_processed_at
            )
        """

        converted_records = []
        now = datetime.now().astimezone().isoformat()

        for record in records:
            converted = {
                "request_timestamp": _to_sqlite_timestamp(
                    record.get("request_timestamp")
                ),
                "request_date": _to_sqlite_date(record.get("request_date")),
                "request_hour": record.get("request_hour"),
                "day_of_week": record.get("day_of_week"),
                "request_uri": record.get("request_uri"),
                "request_host": record.get("request_host"),
                "url_path": record.get("url_path"),
                "url_path_depth": record.get("url_path_depth"),
                "user_agent_raw": record.get("user_agent_raw"),
                "bot_name": record.get("bot_name"),
                "bot_provider": record.get("bot_provider"),
                "bot_category": record.get("bot_category"),
                "bot_score": record.get("bot_score"),
                "is_verified_bot": _to_sqlite_bool(record.get("is_verified_bot")),
                "crawler_country": record.get("crawler_country"),
                "response_status": record.get("response_status"),
                "response_status_category": record.get("response_status_category"),
                "_processed_at": record.get("_processed_at", now),
            }
            converted_records.append(converted)

        with self._cursor() as cursor:
            cursor.executemany(sql, converted_records)
            # executemany may not set rowcount correctly; use len instead
            return len(converted_records)

    def delete_date_range(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> int:
        """
        Delete records within a date range (for reprocessing).

        Args:
            table_name: Table to delete from
            date_column: Date column to filter
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Number of rows deleted
        """
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")

        # Validate date_column to prevent SQL injection
        _validate_identifier(date_column, VALID_DATE_COLUMNS, "date column")

        sql = f"""
            DELETE FROM {table_name}
            WHERE {date_column} >= :start_date
              AND {date_column} <= :end_date
        """
        return self.execute(
            sql,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )

    def vacuum(self) -> None:
        """
        Optimize database by reclaiming space.

        Call after large deletions to reduce file size.
        """
        conn = self._get_connection()
        conn.execute("VACUUM")
        logger.info("Database vacuumed")

    def get_schema_info(self) -> dict[str, list[dict]]:
        """
        Get schema information for all tables.

        Returns:
            Dictionary mapping table names to column info
        """
        tables = self.query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )

        schema_info = {}
        for table in tables:
            table_name = table["name"]
            columns = self.query(f"PRAGMA table_info({table_name})")
            schema_info[table_name] = columns

        return schema_info

    def health_check(self) -> dict:
        """Extended health check with SQLite-specific info."""
        base_check = super().health_check()

        if base_check["healthy"]:
            # Add SQLite-specific details
            try:
                db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
                tables = self.query(
                    "SELECT COUNT(*) as count FROM sqlite_master WHERE type='table'"
                )
                base_check["details"] = {
                    "db_path": str(self.db_path),
                    "db_size_bytes": db_size,
                    "table_count": tables[0]["count"] if tables else 0,
                }
            except Exception as e:
                base_check["details"]["warning"] = str(e)

        return base_check
