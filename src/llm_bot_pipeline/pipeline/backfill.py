"""
SQLite-to-BigQuery backfill module.

Migrates historical data from legacy/POC SQLite databases into BigQuery,
handling schema differences, type conversions, and column enrichment.

Designed to be reusable for any client that needs to backfill BigQuery
from an existing SQLite database (POC or local_sqlite mode).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from ..config.constants import (
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_QUERY_FANOUT_SESSIONS,
    TABLE_RAW_BOT_REQUESTS,
    TABLE_SESSION_URL_DETAILS,
    TABLE_SITEMAP_URLS,
)
from ..utils.bot_classifier import classify_bot_dict

logger = logging.getLogger(__name__)

# Tables to migrate in dependency order
MIGRATION_ORDER = [
    TABLE_RAW_BOT_REQUESTS,
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_QUERY_FANOUT_SESSIONS,
    TABLE_SESSION_URL_DETAILS,
    TABLE_SITEMAP_URLS,
]

# Date columns used for range filtering per table
TABLE_DATE_COLUMNS = {
    TABLE_RAW_BOT_REQUESTS: "EdgeStartTimestamp",
    TABLE_CLEAN_BOT_REQUESTS: "request_date",
    TABLE_QUERY_FANOUT_SESSIONS: "session_date",
    TABLE_SESSION_URL_DETAILS: "session_date",
    TABLE_SITEMAP_URLS: None,
}


@dataclass
class MigrationResult:
    """Result of migrating a single table."""

    table_name: str
    success: bool
    rows_read: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class BackfillCheckpoint:
    """Tracks completed (domain, table, date) tuples for resume."""

    path: Path
    completed: dict[str, set[str]] = field(default_factory=dict)

    def load(self) -> None:
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.completed = {k: set(v) for k, v in data.items()}
            logger.info("Loaded checkpoint with %d table entries", len(self.completed))

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: sorted(v) for k, v in self.completed.items()}
        self.path.write_text(json.dumps(data, indent=2))

    def is_done(self, table: str, date_str: str) -> bool:
        return date_str in self.completed.get(table, set())

    def mark_done(self, table: str, date_str: str) -> None:
        self.completed.setdefault(table, set()).add(date_str)
        self.save()


# ---------------------------------------------------------------------------
# Row mappers — one per table
# ---------------------------------------------------------------------------


def _parse_iso_timestamp(value: Optional[str]) -> Optional[str]:
    """Parse ISO timestamp string to ISO format suitable for BQ TIMESTAMP."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return value


def _iso_to_unix_nanos(value: Optional[str]) -> Optional[int]:
    """Convert ISO 8601 text timestamp to Unix nanoseconds (INT64)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1_000_000_000)
    except (ValueError, TypeError):
        return None


def map_raw_bot_requests(row: dict, domain: str) -> dict:
    """Map a POC raw_bot_requests row to the BigQuery schema."""
    ua = row.get("ClientRequestUserAgent")
    bot_info = classify_bot_dict(ua)

    return {
        "EdgeStartTimestamp": _iso_to_unix_nanos(row.get("EdgeStartTimestamp")),
        "ClientRequestURI": row.get("ClientRequestURI"),
        "ClientRequestHost": row.get("ClientRequestHost"),
        "domain": domain,
        "ClientRequestUserAgent": ua,
        "ClientIP": row.get("ClientIP"),
        "ClientCountry": row.get("ClientCountry"),
        "EdgeResponseStatus": row.get("EdgeResponseStatus"),
        "RayID": None,
        "_bot_name": bot_info["bot_name"],
        "_bot_provider": bot_info["bot_provider"],
        "_bot_category": bot_info["bot_category"],
        "_ingestion_time": _parse_iso_timestamp(row.get("_ingestion_time")),
        "source_provider": row.get("source_provider", "cloudflare"),
    }


def map_bot_requests_daily(row: dict, domain: str) -> dict:
    """Map a POC bot_requests_daily row to the BigQuery schema."""
    return {
        "request_timestamp": _parse_iso_timestamp(row.get("request_timestamp")),
        "request_date": row.get("request_date"),
        "request_hour": row.get("request_hour"),
        "day_of_week": row.get("day_of_week"),
        "request_uri": row.get("request_uri"),
        "request_host": row.get("request_host"),
        "domain": domain,
        "url_path": row.get("url_path"),
        "url_path_depth": row.get("url_path_depth"),
        "user_agent_raw": row.get("user_agent_raw"),
        "bot_name": row.get("bot_name"),
        "bot_provider": row.get("bot_provider"),
        "bot_category": row.get("bot_category"),
        "crawler_country": row.get("crawler_country"),
        "response_status": row.get("response_status"),
        "response_status_category": row.get("response_status_category"),
        "_processed_at": _parse_iso_timestamp(row.get("_processed_at")),
    }


def map_query_fanout_sessions(row: dict, domain: str) -> dict:
    """Map a POC query_fanout_sessions row to the BigQuery schema."""
    return {
        "session_id": row.get("session_id"),
        "session_date": row.get("session_date"),
        "domain": domain,
        "session_start_time": _parse_iso_timestamp(row.get("session_start_time")),
        "session_end_time": _parse_iso_timestamp(row.get("session_end_time")),
        "duration_ms": row.get("duration_ms"),
        "bot_provider": row.get("bot_provider"),
        "bot_name": row.get("bot_name"),
        "request_count": row.get("request_count"),
        "unique_urls": row.get("unique_urls"),
        "mean_cosine_similarity": row.get("mean_cosine_similarity"),
        "min_cosine_similarity": row.get("min_cosine_similarity"),
        "max_cosine_similarity": row.get("max_cosine_similarity"),
        "confidence_level": row.get("confidence_level"),
        "fanout_session_name": row.get("fanout_session_name"),
        "url_list": row.get("url_list"),
        "window_ms": row.get("window_ms"),
        "splitting_strategy": None,
        "_created_at": _parse_iso_timestamp(row.get("_created_at")),
        "parent_session_id": None,
        "was_refined": None,
        "refinement_reason": None,
        "pre_refinement_mibcs": None,
    }


def map_session_url_details(
    row: dict, domain: str, session_lookup: dict[str, dict]
) -> dict:
    """Map a POC session_url_details row, enriching from sessions lookup."""
    sid = row.get("session_id")
    session = session_lookup.get(sid, {})
    return {
        "session_id": sid,
        "session_date": row.get("session_date"),
        "domain": domain,
        "url": row.get("url"),
        "url_position": row.get("url_position"),
        "bot_provider": row.get("bot_provider"),
        "bot_name": row.get("bot_name"),
        "fanout_session_name": row.get("fanout_session_name"),
        "confidence_level": row.get("confidence_level"),
        "session_request_count": row.get("session_request_count"),
        "session_unique_urls": row.get("session_unique_urls"),
        "session_duration_ms": row.get("session_duration_ms"),
        "mean_cosine_similarity": row.get("mean_cosine_similarity"),
        "min_cosine_similarity": row.get("min_cosine_similarity"),
        "max_cosine_similarity": row.get("max_cosine_similarity"),
        "session_start_time": _parse_iso_timestamp(session.get("session_start_time")),
        "session_end_time": _parse_iso_timestamp(session.get("session_end_time")),
        "window_ms": session.get("window_ms"),
        "splitting_strategy": None,
        "_created_at": _parse_iso_timestamp(row.get("_created_at")),
    }


def map_sitemap_urls(row: dict) -> dict:
    """Map a POC sitemap_urls row to the BigQuery schema."""
    return {
        "url": row.get("url"),
        "url_path": row.get("url_path"),
        "lastmod": row.get("lastmod"),
        "lastmod_month": row.get("lastmod_month"),
        "sitemap_source": row.get("sitemap_source"),
        "_fetched_at": _parse_iso_timestamp(row.get("_fetched_at")),
    }


# Mapper registry
TABLE_MAPPERS = {
    TABLE_RAW_BOT_REQUESTS: map_raw_bot_requests,
    TABLE_CLEAN_BOT_REQUESTS: map_bot_requests_daily,
    TABLE_QUERY_FANOUT_SESSIONS: map_query_fanout_sessions,
    TABLE_SITEMAP_URLS: map_sitemap_urls,
}


# ---------------------------------------------------------------------------
# Core backfill manager
# ---------------------------------------------------------------------------


class SQLiteBackfillManager:
    """Migrates data from a legacy/POC SQLite database to BigQuery.

    Reads from the source SQLite DB, applies per-table column mapping and type
    conversion, then bulk-loads into BigQuery via the BigQueryBackend.

    Usage::

        from llm_bot_pipeline.storage import get_backend
        bq = get_backend("bigquery", project_id="my-project")
        bq.initialize()

        mgr = SQLiteBackfillManager(
            sqlite_path=Path("/data/example.db"),
            domain="example.com",
            bq_backend=bq,
        )
        results = mgr.migrate_all()
        for table, result in results.items():
            print(f"{table}: {result.rows_written} rows")
    """

    def __init__(
        self,
        sqlite_path: Path,
        domain: str,
        bq_backend,
        batch_size: int = 1000,
        checkpoint_path: Optional[Path] = None,
    ):
        self.sqlite_path = Path(sqlite_path)
        self.domain = domain
        self.bq = bq_backend
        self.batch_size = batch_size
        self._conn: Optional[sqlite3.Connection] = None
        self._session_lookup: Optional[dict[str, dict]] = None
        self.checkpoint = BackfillCheckpoint(
            path=checkpoint_path or Path("data/backfill_checkpoint.json")
        )
        self.checkpoint.load()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.sqlite_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def detect_schema_version(self) -> str:
        """Detect whether source DB uses POC or dev schema.

        Returns 'poc' if raw_bot_requests lacks a 'domain' column, 'dev' otherwise.
        """
        cursor = self.conn.execute("PRAGMA table_info(raw_bot_requests)")
        columns = {row["name"] for row in cursor.fetchall()}
        return "dev" if "domain" in columns else "poc"

    def get_tables(self) -> list[str]:
        """Return list of non-empty user tables in the SQLite database."""
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = []
        for row in cursor.fetchall():
            name = row["name"]
            count = self.conn.execute(f"SELECT COUNT(*) as c FROM [{name}]").fetchone()[
                "c"
            ]
            if count > 0:
                tables.append(name)
        return tables

    def get_table_row_count(self, table: str) -> int:
        row = self.conn.execute(f"SELECT COUNT(*) as c FROM [{table}]").fetchone()
        return row["c"]

    def get_date_range(self, table: str) -> tuple[Optional[str], Optional[str]]:
        """Return (min_date, max_date) for a table, or (None, None)."""
        date_col = TABLE_DATE_COLUMNS.get(table)
        if not date_col:
            return None, None
        row = self.conn.execute(
            f"SELECT MIN({date_col}) as mn, MAX({date_col}) as mx FROM [{table}]"
        ).fetchone()
        return row["mn"], row["mx"]

    def get_distinct_dates(self, table: str) -> list[str]:
        """Return sorted list of distinct date strings for a table."""
        date_col = TABLE_DATE_COLUMNS.get(table)
        if not date_col:
            return []
        if table == TABLE_RAW_BOT_REQUESTS:
            sql = f"SELECT DISTINCT DATE({date_col}) as d FROM [{table}] ORDER BY d"
        else:
            sql = f"SELECT DISTINCT {date_col} as d FROM [{table}] ORDER BY d"
        return [row["d"] for row in self.conn.execute(sql).fetchall() if row["d"]]

    # ------------------------------------------------------------------
    # Session lookup (for session_url_details enrichment)
    # ------------------------------------------------------------------

    def _build_session_lookup(self) -> dict[str, dict]:
        """Build {session_id: {session_start_time, session_end_time, window_ms}} lookup."""
        if self._session_lookup is not None:
            return self._session_lookup
        self._session_lookup = {}
        cursor = self.conn.execute(
            "SELECT session_id, session_start_time, session_end_time, window_ms "
            "FROM query_fanout_sessions"
        )
        for row in cursor.fetchall():
            self._session_lookup[row["session_id"]] = {
                "session_start_time": row["session_start_time"],
                "session_end_time": row["session_end_time"],
                "window_ms": row["window_ms"],
            }
        logger.info("Built session lookup with %d entries", len(self._session_lookup))
        return self._session_lookup

    # ------------------------------------------------------------------
    # Table migration
    # ------------------------------------------------------------------

    def _read_rows(self, table: str, date_value: Optional[str] = None) -> list[dict]:
        """Read rows from SQLite, optionally filtered to a single date."""
        date_col = TABLE_DATE_COLUMNS.get(table)
        if date_value and date_col:
            if table == TABLE_RAW_BOT_REQUESTS:
                sql = f"SELECT * FROM [{table}] " f"WHERE DATE({date_col}) = ?"
            else:
                sql = f"SELECT * FROM [{table}] WHERE {date_col} = ?"
            cursor = self.conn.execute(sql, (date_value,))
        else:
            cursor = self.conn.execute(f"SELECT * FROM [{table}]")
        return [dict(row) for row in cursor.fetchall()]

    def _map_rows(self, table: str, rows: list[dict]) -> list[dict]:
        """Apply table-specific mapper to rows."""
        if table == TABLE_SESSION_URL_DETAILS:
            lookup = self._build_session_lookup()
            return [map_session_url_details(r, self.domain, lookup) for r in rows]
        if table == TABLE_SITEMAP_URLS:
            return [map_sitemap_urls(r) for r in rows]

        mapper = TABLE_MAPPERS.get(table)
        if mapper:
            return [mapper(r, self.domain) for r in rows]

        raise ValueError(f"No mapper for table: {table}")

    def _insert_batch(self, table: str, records: list[dict]) -> int:
        """Insert a batch of records into BigQuery via the backend."""
        if not records:
            return 0
        return self.bq.insert_records(table, records)

    def migrate_table(
        self,
        table_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> MigrationResult:
        """Migrate a single table from SQLite to BigQuery.

        Processes day-by-day for date-partitioned tables, or in a single batch
        for non-partitioned tables (sitemap_urls).
        """
        started = datetime.now(timezone.utc)
        logger.info(
            "Migrating table: %s (domain=%s, dry_run=%s)",
            table_name,
            self.domain,
            dry_run,
        )

        try:
            source_count = self.get_table_row_count(table_name)
            if source_count == 0:
                logger.info("Table %s is empty, skipping", table_name)
                return MigrationResult(
                    table_name=table_name,
                    success=True,
                    duration_seconds=(
                        datetime.now(timezone.utc) - started
                    ).total_seconds(),
                )

            date_col = TABLE_DATE_COLUMNS.get(table_name)
            dates = self.get_distinct_dates(table_name) if date_col else [None]

            if start_date and date_col:
                dates = [d for d in dates if d and d >= str(start_date)]
            if end_date and date_col:
                dates = [d for d in dates if d and d <= str(end_date)]

            total_read = 0
            total_written = 0
            total_skipped = 0

            for date_val in dates:
                checkpoint_key = f"{self.domain}:{date_val or 'all'}"
                if not force and self.checkpoint.is_done(table_name, checkpoint_key):
                    logger.debug(
                        "Skipping %s date=%s (already done)", table_name, date_val
                    )
                    total_skipped += 1
                    continue

                if force and date_val and not dry_run:
                    bq_date_col = TABLE_DATE_COLUMNS.get(table_name, "request_date")
                    if bq_date_col and date_val:
                        try:
                            d = date.fromisoformat(date_val[:10])
                            deleted = self.bq.delete_date_range(
                                table_name, bq_date_col, d, d
                            )
                            if deleted:
                                logger.info(
                                    "Force-deleted %d rows for %s/%s",
                                    deleted,
                                    table_name,
                                    date_val,
                                )
                        except Exception as e:
                            logger.warning(
                                "Could not delete existing data for %s/%s: %s",
                                table_name,
                                date_val,
                                e,
                            )

                rows = self._read_rows(table_name, date_val)
                total_read += len(rows)

                mapped = self._map_rows(table_name, rows)

                if dry_run:
                    logger.info(
                        "[DRY-RUN] %s date=%s: would write %d rows",
                        table_name,
                        date_val,
                        len(mapped),
                    )
                    total_written += len(mapped)
                    continue

                for i in range(0, len(mapped), self.batch_size):
                    batch = mapped[i : i + self.batch_size]
                    written = self._insert_batch(table_name, batch)
                    total_written += written

                self.checkpoint.mark_done(table_name, checkpoint_key)
                logger.info(
                    "%s date=%s: wrote %d rows", table_name, date_val, len(mapped)
                )

            duration = (datetime.now(timezone.utc) - started).total_seconds()
            logger.info(
                "Completed %s: read=%d written=%d skipped=%d in %.1fs",
                table_name,
                total_read,
                total_written,
                total_skipped,
                duration,
            )
            return MigrationResult(
                table_name=table_name,
                success=True,
                rows_read=total_read,
                rows_written=total_written,
                rows_skipped=total_skipped,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - started).total_seconds()
            logger.exception("Failed to migrate %s: %s", table_name, e)
            return MigrationResult(
                table_name=table_name,
                success=False,
                rows_read=0,
                error=str(e),
                duration_seconds=duration,
            )

    def migrate_all(
        self,
        tables: Optional[list[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> dict[str, MigrationResult]:
        """Migrate multiple tables in dependency order.

        Args:
            tables: Specific tables to migrate (default: all populated tables).
            start_date: Optional start date filter.
            end_date: Optional end date filter.
            dry_run: If True, validate and log without writing.
            force: If True, delete existing BQ data before inserting.

        Returns:
            Dict mapping table name to MigrationResult.
        """
        available = set(self.get_tables())
        if tables:
            to_migrate = [t for t in MIGRATION_ORDER if t in tables and t in available]
        else:
            to_migrate = [t for t in MIGRATION_ORDER if t in available]

        logger.info(
            "Starting backfill: domain=%s tables=%s dates=%s..%s dry_run=%s",
            self.domain,
            to_migrate,
            start_date,
            end_date,
            dry_run,
        )

        schema_version = self.detect_schema_version()
        logger.info("Detected schema version: %s", schema_version)

        results: dict[str, MigrationResult] = {}
        for table in to_migrate:
            result = self.migrate_table(
                table,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                force=force,
            )
            results[table] = result
            if not result.success:
                logger.error("Stopping migration — %s failed: %s", table, result.error)
                break

        successful = sum(1 for r in results.values() if r.success)
        total_rows = sum(r.rows_written for r in results.values())
        logger.info(
            "Backfill complete: %d/%d tables, %d total rows written",
            successful,
            len(results),
            total_rows,
        )
        return results

    def print_summary(self, results: dict[str, MigrationResult]) -> None:
        """Print a formatted summary of migration results."""
        print(f"\n{'='*60}")
        print(f"Backfill Summary — {self.domain}")
        print(f"Source: {self.sqlite_path}")
        print(f"{'='*60}")
        for table, result in results.items():
            status = "OK" if result.success else "FAILED"
            print(
                f"  {table:<30s} {status:>6s}  "
                f"read={result.rows_read:>7,}  written={result.rows_written:>7,}  "
                f"skipped={result.rows_skipped:>3}  {result.duration_seconds:.1f}s"
            )
            if result.error:
                print(f"    ERROR: {result.error}")
        total = sum(r.rows_written for r in results.values())
        print(f"{'─'*60}")
        print(f"  Total rows written: {total:,}")
        print()
