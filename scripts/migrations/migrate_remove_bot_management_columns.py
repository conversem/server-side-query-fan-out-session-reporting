#!/usr/bin/env python3
"""
Migration script: remove legacy Enterprise Bot Management columns (v2.0.x → v2.1.1+).

Drops the following columns that were removed in v2.1.1 after Cloudflare
Enterprise Bot Management was found to be unavailable in most deployment
environments. LLM bot classification is now done entirely post-ingestion
via user-agent pattern matching.

Columns dropped:
  bot_requests_daily:  bot_score, is_verified_bot
  daily_summary:       avg_bot_score

These columns are harmless if left in place (the pipeline ignores them), so
this migration is optional. Run it for a clean schema and to reclaim space.

Requires SQLite >= 3.35.0 (Python 3.9+ ships with a compatible version).

Usage:
    python scripts/migrations/migrate_remove_bot_management_columns.py
    python scripts/migrations/migrate_remove_bot_management_columns.py --db-path data/llm-bot-logs.db
    python scripts/migrations/migrate_remove_bot_management_columns.py --dry-run
    python scripts/migrations/migrate_remove_bot_management_columns.py --db-path "data/*.db"
"""

import argparse
import glob
import logging
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Columns to drop: {table: [column, ...]}
COLUMNS_TO_REMOVE = {
    "bot_requests_daily": ["bot_score", "is_verified_bot"],
    "daily_summary": ["avg_bot_score"],
}


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


def _sqlite_version(conn: sqlite3.Connection) -> tuple[int, ...]:
    version_str = conn.execute("SELECT sqlite_version()").fetchone()[0]
    return tuple(int(x) for x in version_str.split("."))


def migrate_sqlite(db_path: Path, dry_run: bool = False) -> None:
    if not db_path.exists():
        logger.warning("Database not found: %s — skipping", db_path)
        return

    conn = sqlite3.connect(db_path)
    try:
        version = _sqlite_version(conn)
        if version < (3, 35, 0):
            logger.error(
                "SQLite %s detected. ALTER TABLE DROP COLUMN requires >= 3.35.0. "
                "Upgrade Python/SQLite or remove columns manually.",
                ".".join(str(v) for v in version),
            )
            sys.exit(1)

        any_change = False
        for table, columns in COLUMNS_TO_REMOVE.items():
            existing = _table_columns(conn, table)
            to_drop = [c for c in columns if c in existing]
            if not to_drop:
                logger.info(
                    "[%s] All target columns already absent — nothing to do", table
                )
                continue

            for col in to_drop:
                if dry_run:
                    logger.info("[dry-run] Would DROP COLUMN %s.%s", table, col)
                else:
                    logger.info("Dropping %s.%s ...", table, col)
                    conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
                any_change = True

        if any_change and not dry_run:
            conn.commit()
            conn.execute("VACUUM")
            logger.info("Migration complete for %s", db_path)
        elif not any_change:
            logger.info("Nothing to migrate in %s", db_path)
        else:
            logger.info("[dry-run] No changes applied to %s", db_path)

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove legacy Enterprise Bot Management columns from SQLite database."
    )
    parser.add_argument(
        "--db-path",
        default="data/llm-bot-logs.db",
        help="Path to SQLite database or glob pattern (default: data/llm-bot-logs.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without applying them",
    )
    args = parser.parse_args()

    db_paths = [Path(p) for p in glob.glob(args.db_path)] or [Path(args.db_path)]
    if not db_paths:
        logger.error("No database files found matching: %s", args.db_path)
        sys.exit(1)

    for db_path in sorted(db_paths):
        migrate_sqlite(db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
