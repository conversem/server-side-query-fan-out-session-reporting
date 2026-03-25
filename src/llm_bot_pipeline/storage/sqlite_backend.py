"""
SQLite storage backend implementation.

Provides a local SQLite-based storage for local development,
with schema matching the SQLite production structure.
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Register explicit sqlite3 adapters for date/datetime (Python 3.12+).
# The built-in default adapters are deprecated; these replacements follow
# the recipes from the official sqlite3 documentation.
# ---------------------------------------------------------------------------
sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

from ..config.constants import VALID_TABLE_NAMES
from .base import (
    BackendCapabilities,
    QueryError,
    SchemaError,
    StorageBackend,
    StorageConnectionError,
    validate_date_column,
    validate_table_name,
)
from .disk_space import check_disk_space
from .sqlite_schemas import (
    CLEAN_TABLE_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    DATA_FRESHNESS_SCHEMA,
    INDEX_DEFINITIONS,
    QUERY_FANOUT_SESSIONS_NATURAL_KEY_INDEX,
    QUERY_FANOUT_SESSIONS_SCHEMA,
    RAW_TABLE_SCHEMA,
    SESSION_REFINEMENT_LOG_SCHEMA,
    SESSION_URL_DETAILS_SCHEMA,
    SITEMAP_FRESHNESS_SCHEMA,
    SITEMAP_URLS_SCHEMA,
    URL_PERFORMANCE_SCHEMA,
    URL_VOLUME_DECAY_SCHEMA,
    VIEW_DEFINITIONS,
    VIEW_NAMES,
)

logger = logging.getLogger(__name__)


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

VALID_TABLES = VALID_TABLE_NAMES


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
        disk_space_threshold_mb: int = 500,
        vacuum_threshold: int = 10_000,
    ):
        """
        Initialize SQLite backend.

        Args:
            db_path: Path to SQLite database file
            check_same_thread: SQLite check_same_thread parameter
            timeout: Connection timeout in seconds
            disk_space_threshold_mb: Minimum free MB required before writes
            vacuum_threshold: Run VACUUM after deletes exceed this row count (0=disabled)
        """
        self.db_path = Path(db_path)
        self._check_same_thread = check_same_thread
        self._timeout = timeout
        self._disk_space_threshold_mb = disk_space_threshold_mb
        self._vacuum_threshold = vacuum_threshold
        self._connection: Optional[sqlite3.Connection] = None
        self._deleted_since_vacuum = 0

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def backend_type(self) -> str:
        """Return backend type identifier."""
        return "sqlite"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_sql=True,
            supports_streaming=False,
            supports_partitioning=False,
            supports_transactions=True,
            supports_upsert=True,
            parameter_style="named",
        )

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

    # ------------------------------------------------------------------
    # Schema migration constants for v1 → v2 auto-upgrade
    # ------------------------------------------------------------------
    _V2_MIGRATIONS: list[tuple[str, str, str]] = [
        ("raw_bot_requests", "domain", "TEXT"),
        ("raw_bot_requests", "RayID", "TEXT"),
        ("bot_requests_daily", "domain", "TEXT"),
        ("daily_summary", "domain", "TEXT"),
        ("url_performance", "domain", "TEXT"),
        ("query_fanout_sessions", "domain", "TEXT"),
        ("query_fanout_sessions", "splitting_strategy", "TEXT"),
        ("query_fanout_sessions", "parent_session_id", "TEXT"),
        ("query_fanout_sessions", "was_refined", "INTEGER NOT NULL DEFAULT 0"),
        ("query_fanout_sessions", "refinement_reason", "TEXT"),
        ("query_fanout_sessions", "pre_refinement_mibcs", "REAL"),
    ]

    def _migrate_schema(self, cursor: sqlite3.Cursor) -> None:
        """
        Auto-migrate v1 schema to v2 by adding missing columns.

        Idempotent: checks PRAGMA table_info before each ALTER TABLE.
        Only runs when the v1 sentinel (missing domain on raw_bot_requests)
        is detected on an existing database.
        """

        def _column_exists(table: str, column: str) -> bool:
            cursor.execute(f"PRAGMA table_info({table})")
            return any(row[1] == column for row in cursor.fetchall())

        def _table_exists(table: str) -> bool:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            return cursor.fetchone() is not None

        # Sentinel: if raw_bot_requests exists but has no domain column → v1
        if not _table_exists("raw_bot_requests"):
            return
        if _column_exists("raw_bot_requests", "domain"):
            return

        logger.info("Auto-migrating SQLite schema from v1 to v2...")
        for table, column, column_def in self._V2_MIGRATIONS:
            if not _table_exists(table):
                continue
            if _column_exists(table, column):
                continue
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_def}")
            logger.debug("Added %s.%s", table, column)
        logger.info("Schema auto-migration complete")

    def initialize(self) -> None:
        """
        Initialize database with all required tables, indexes, and views.

        Safe to call multiple times — uses IF NOT EXISTS. Automatically
        migrates v1 databases by adding missing columns before creating
        new tables and indexes.
        """
        logger.info(f"Initializing SQLite database: {self.db_path}")

        with self._cursor() as cursor:
            # Phase 1: Migrate existing tables (adds missing v2 columns)
            self._migrate_schema(cursor)

            # Create tables
            cursor.execute(RAW_TABLE_SCHEMA)
            cursor.execute(CLEAN_TABLE_SCHEMA)
            cursor.execute(DAILY_SUMMARY_SCHEMA)
            cursor.execute(URL_PERFORMANCE_SCHEMA)
            cursor.execute(DATA_FRESHNESS_SCHEMA)
            cursor.execute(QUERY_FANOUT_SESSIONS_SCHEMA)
            cursor.execute(QUERY_FANOUT_SESSIONS_NATURAL_KEY_INDEX)
            cursor.execute(SESSION_URL_DETAILS_SCHEMA)
            cursor.execute(SESSION_REFINEMENT_LOG_SCHEMA)
            cursor.execute(SITEMAP_URLS_SCHEMA)
            cursor.execute(SITEMAP_FRESHNESS_SCHEMA)
            cursor.execute(URL_VOLUME_DECAY_SCHEMA)

            # Create indexes
            for index_sql in INDEX_DEFINITIONS:
                cursor.execute(index_sql)

            # Drop and recreate reporting views (ensures schema changes propagate)
            for view_name in VIEW_NAMES:
                cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
            for view_sql in VIEW_DEFINITIONS:
                cursor.execute(view_sql)

        logger.info("SQLite database initialized successfully")

    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug("SQLite connection closed")

    def _check_disk_space(self) -> None:
        """Validate sufficient disk space before write operations."""
        check_disk_space(self.db_path, self._disk_space_threshold_mb)

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

        self._check_disk_space()

        sql = """
            INSERT INTO raw_bot_requests (
                EdgeStartTimestamp, ClientRequestURI, ClientRequestHost,
                ClientRequestUserAgent, ClientIP, ClientCountry,
                EdgeResponseStatus, RayID, _ingestion_time,
                source_provider, domain
            ) VALUES (
                :EdgeStartTimestamp, :ClientRequestURI, :ClientRequestHost,
                :ClientRequestUserAgent, :ClientIP, :ClientCountry,
                :EdgeResponseStatus, :RayID, :_ingestion_time,
                :source_provider, :domain
            )
        """

        # Convert records for SQLite
        converted_records = []
        now = datetime.now(timezone.utc).isoformat()

        for record in records:
            converted = {
                "EdgeStartTimestamp": _to_sqlite_timestamp(
                    record.get("EdgeStartTimestamp")
                ),
                "ClientRequestURI": record.get("ClientRequestURI"),
                "ClientRequestHost": record.get("ClientRequestHost"),
                "ClientRequestUserAgent": record.get("ClientRequestUserAgent"),
                "ClientIP": record.get("ClientIP"),
                "ClientCountry": record.get("ClientCountry"),
                "EdgeResponseStatus": record.get("EdgeResponseStatus"),
                "RayID": record.get("RayID"),
                "_ingestion_time": record.get("_ingestion_time", now),
                "source_provider": record.get("source_provider"),
                "domain": record.get("domain"),
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
            rowcount = cursor.rowcount

        # Track deletes and vacuum when threshold exceeded
        if (
            self._vacuum_threshold > 0
            and sql.strip().upper().startswith("DELETE")
            and rowcount > 0
        ):
            self._deleted_since_vacuum += rowcount
            if self._deleted_since_vacuum >= self._vacuum_threshold:
                self.vacuum()
                self._deleted_since_vacuum = 0

        return rowcount

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
        validate_table_name(table_name)
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")

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
        validate_table_name(table_name)
        validate_date_column(date_column)
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")

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

        self._check_disk_space()

        sql = """
            INSERT INTO bot_requests_daily (
                request_timestamp, request_date, request_hour, day_of_week,
                request_uri, request_host, domain, url_path, url_path_depth,
                user_agent_raw, bot_name, bot_provider, bot_category,
                crawler_country, response_status,
                response_status_category, resource_type, _processed_at
            ) VALUES (
                :request_timestamp, :request_date, :request_hour, :day_of_week,
                :request_uri, :request_host, :domain, :url_path, :url_path_depth,
                :user_agent_raw, :bot_name, :bot_provider, :bot_category,
                :crawler_country, :response_status,
                :response_status_category, :resource_type, :_processed_at
            )
        """

        converted_records = []
        now = datetime.now(timezone.utc).isoformat()

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
                "domain": record.get("domain"),
                "url_path": record.get("url_path"),
                "url_path_depth": record.get("url_path_depth"),
                "user_agent_raw": record.get("user_agent_raw"),
                "bot_name": record.get("bot_name"),
                "bot_provider": record.get("bot_provider"),
                "bot_category": record.get("bot_category"),
                "crawler_country": record.get("crawler_country"),
                "response_status": record.get("response_status"),
                "response_status_category": record.get("response_status_category"),
                "resource_type": record.get("resource_type", "document"),
                "_processed_at": record.get("_processed_at", now),
            }
            converted_records.append(converted)

        with self._cursor() as cursor:
            cursor.executemany(sql, converted_records)
            # executemany may not set rowcount correctly; use len instead
            return len(converted_records)

    def insert_sitemap_urls(self, entries: list[dict]) -> int:
        """Insert or replace sitemap URL entries into sitemap_urls table.

        Uses INSERT OR REPLACE so re-fetching a sitemap updates lastmod values.

        Args:
            entries: List of dicts with keys: url, url_path, lastmod,
                     lastmod_month, sitemap_source

        Returns:
            Number of entries upserted
        """
        if not entries:
            return 0

        self._check_disk_space()

        sql = """
            INSERT OR REPLACE INTO sitemap_urls
                (url, url_path, lastmod, lastmod_month, sitemap_source, _fetched_at)
            VALUES
                (:url, :url_path, :lastmod, :lastmod_month, :sitemap_source, datetime('now'))
        """

        with self._cursor() as cursor:
            cursor.executemany(sql, entries)
            return len(entries)

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
        validate_table_name(table_name)
        validate_date_column(date_column)
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")

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
        logger.info("SQLite VACUUM completed to reclaim disk space")

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
