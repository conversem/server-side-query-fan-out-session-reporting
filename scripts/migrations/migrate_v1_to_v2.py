#!/usr/bin/env python3
"""
Migration script: SQLite schema v1 → v2.

Upgrades an existing v1 database (from the original public release) to the v2
schema used in the current release. The migration is idempotent — running it
multiple times on an already-migrated database is safe.

What changes:
  - Adds 11 new columns to 5 existing tables (domain + session refinement fields)
  - Creates 5 new tables (handled by initialize())
  - Creates 10 new views (handled by initialize())
  - Creates 17 new indexes (handled by initialize())

New columns added:
  raw_bot_requests:       domain, RayID
  bot_requests_daily:     domain
  daily_summary:          domain
  url_performance:        domain
  query_fanout_sessions:  domain, splitting_strategy, parent_session_id,
                          was_refined, refinement_reason, pre_refinement_mibcs

Usage:
    python scripts/migrations/migrate_v1_to_v2.py [options]

Options:
    --db-path PATH    Path to SQLite database (default: data/llm-bot-logs.db)
    --dry-run         Show what would be done without making changes
    --backfill-domain Populate domain from ClientRequestHost (single-domain users)
    --verbose         Enable verbose output

Multi-domain:
    python scripts/migrations/migrate_v1_to_v2.py --db-path "data/*.db"
"""

import argparse
import glob
import logging
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# New columns to add per table
# Each entry: (table_name, column_name, column_def)
# ---------------------------------------------------------------------------
NEW_COLUMNS = [
    # raw_bot_requests
    ("raw_bot_requests", "domain", "TEXT"),
    ("raw_bot_requests", "RayID", "TEXT"),
    # bot_requests_daily
    ("bot_requests_daily", "domain", "TEXT"),
    # daily_summary
    ("daily_summary", "domain", "TEXT"),
    # url_performance
    ("url_performance", "domain", "TEXT"),
    # query_fanout_sessions
    ("query_fanout_sessions", "domain", "TEXT"),
    ("query_fanout_sessions", "splitting_strategy", "TEXT"),
    ("query_fanout_sessions", "parent_session_id", "TEXT"),
    ("query_fanout_sessions", "was_refined", "INTEGER NOT NULL DEFAULT 0"),
    ("query_fanout_sessions", "refinement_reason", "TEXT"),
    ("query_fanout_sessions", "pre_refinement_mibcs", "REAL"),
]


def column_exists(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    """Check if a column already exists in a table."""
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def table_exists(cursor: sqlite3.Cursor, table: str) -> bool:
    """Check if a table exists in the database."""
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cursor.fetchone() is not None


def is_v1_schema(cursor: sqlite3.Cursor) -> bool:
    """
    Detect whether this is a v1 database that needs migration.

    Sentinel: v1 databases have raw_bot_requests but lack the domain column.
    A fresh database (no tables yet) does not need migration.
    """
    if not table_exists(cursor, "raw_bot_requests"):
        return False
    return not column_exists(cursor, "raw_bot_requests", "domain")


def run_migration(
    db_path: Path,
    dry_run: bool = False,
    backfill_domain: bool = False,
    verbose: bool = False,
) -> bool:
    """
    Run the v1→v2 migration on a single database.

    Args:
        db_path: Path to the SQLite database file.
        dry_run: If True, report what would be done without modifying the DB.
        backfill_domain: If True, populate domain column from ClientRequestHost
                         on raw_bot_requests (useful for single-domain users).
        verbose: If True, log each column check.

    Returns:
        True if migration succeeded (or was a no-op), False on error.
    """
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        return False

    logger.info("Migrating: %s", db_path)

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        if not is_v1_schema(cursor):
            logger.info("  Already at v2 schema (or no tables yet) — skipping.")
            conn.close()
            return True

        logger.info("  Detected v1 schema — applying v2 migration...")

        columns_added = 0
        columns_skipped = 0

        # Phase 1: Add missing columns to existing tables
        for table, column, column_def in NEW_COLUMNS:
            if not table_exists(cursor, table):
                if verbose:
                    logger.info(
                        "  Table %s does not exist — skipping %s", table, column
                    )
                continue

            if column_exists(cursor, table, column):
                if verbose:
                    logger.info(
                        "  Column %s.%s already exists — skipping", table, column
                    )
                columns_skipped += 1
                continue

            sql = f"ALTER TABLE {table} ADD COLUMN {column} {column_def}"
            if dry_run:
                logger.info("  [dry-run] Would execute: %s", sql)
            else:
                cursor.execute(sql)
                logger.info("  + Added %s.%s", table, column)
            columns_added += 1

        # Phase 2: Optional domain backfill from ClientRequestHost
        if backfill_domain and not dry_run:
            logger.info("  Backfilling domain from ClientRequestHost...")
            cursor.execute("""
                UPDATE raw_bot_requests
                SET domain = ClientRequestHost
                WHERE domain IS NULL AND ClientRequestHost IS NOT NULL
                """)
            affected = cursor.rowcount
            logger.info("  Backfilled domain for %d raw_bot_requests rows", affected)

        if not dry_run:
            conn.commit()

        conn.close()

        # Phase 3: Call initialize() to create new tables, views, and indexes
        if not dry_run:
            _run_initialize(db_path)

        action = "Would add" if dry_run else "Added"
        logger.info(
            "  %s %d column(s), skipped %d already-present",
            action,
            columns_added,
            columns_skipped,
        )
        return True

    except Exception as e:
        logger.error("  Migration failed for %s: %s", db_path, e)
        if "conn" in dir():
            conn.close()
        return False


def _run_initialize(db_path: Path) -> None:
    """Call SQLiteBackend.initialize() to create new tables, views, and indexes."""
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
        from llm_bot_pipeline.storage.factory import get_backend

        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()
        backend.close()
        logger.info("  Ran initialize() — new tables, views, and indexes created")
    except Exception as e:
        logger.warning(
            "  Could not run initialize() via SDK (%s). "
            "New tables/views/indexes may not be created. "
            "Run the pipeline once to trigger initialization.",
            e,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate SQLite database from v1 to v2 schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db-path",
        default="data/llm-bot-logs.db",
        help="Path to SQLite database (supports glob for multi-domain, default: data/llm-bot-logs.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--backfill-domain",
        action="store_true",
        help="Populate domain column from ClientRequestHost (single-domain users)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN MODE — no changes will be made ===")

    # Expand glob patterns for multi-domain setups
    db_paths = [Path(p) for p in glob.glob(args.db_path)]
    if not db_paths:
        db_paths = [Path(args.db_path)]

    if len(db_paths) > 1:
        logger.info("Found %d databases to migrate.", len(db_paths))

    success_count = 0
    failure_count = 0

    for db_path in db_paths:
        success = run_migration(
            db_path=db_path,
            dry_run=args.dry_run,
            backfill_domain=args.backfill_domain,
            verbose=args.verbose,
        )
        if success:
            success_count += 1
        else:
            failure_count += 1

    logger.info(
        "Migration complete: %d succeeded, %d failed.",
        success_count,
        failure_count,
    )

    if failure_count > 0:
        return 1

    if args.dry_run:
        logger.info("Re-run without --dry-run to apply changes.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
