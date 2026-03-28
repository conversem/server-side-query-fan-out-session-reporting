#!/usr/bin/env python3
"""
Migration script: add domain column to sitemap tables (v2.1.1 → v2.1.2).

Adds domain TEXT to sitemap_urls, sitemap_freshness, and url_volume_decay.
Backfills domain from sitemap_source for sitemap_urls and sitemap_freshness.
url_volume_decay has no sitemap_source — re-run SitemapAggregator after this.

Usage:
    python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --db-path data/llm-bot-logs.db
    python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --dry-run
"""

import argparse
import glob
import logging
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urlparse

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _extract_domain(sitemap_source: str) -> str:
    """Extract domain from sitemap URL, stripping www. prefix."""
    hostname = urlparse(sitemap_source).hostname or ""
    return hostname.removeprefix("www.")


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, col_def: str, dry_run: bool
) -> bool:
    """Add column to table if it does not already exist. Returns True if added."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column in cols:
        logger.debug("Column %s.%s already exists — skipping", table, column)
        return False
    sql = f"ALTER TABLE {table} ADD COLUMN {column} {col_def}"
    if dry_run:
        logger.info("[DRY RUN] Would execute: %s", sql)
    else:
        conn.execute(sql)
        logger.info("Added column %s.%s", table, column)
    return True


def _extract_domain_sql_expr() -> str:
    """Return the SQLite expression for hostname extraction from sitemap_source.

    Note: uses REPLACE to strip 'www.' which matches the prefix anywhere in
    the hostname. For all realistic sitemap URLs (www. only appears as prefix)
    this is equivalent to Python's str.removeprefix('www.').
    """
    return """REPLACE(REPLACE(REPLACE(
        SUBSTR(sitemap_source,
            INSTR(sitemap_source, '://') + 3,
            CASE
                WHEN INSTR(SUBSTR(sitemap_source, INSTR(sitemap_source, '://') + 3), '/') > 0
                THEN INSTR(SUBSTR(sitemap_source, INSTR(sitemap_source, '://') + 3), '/') - 1
                ELSE LENGTH(sitemap_source)
            END
        ),
    'www.', ''), 'WWW.', ''), 'Www.', '')"""


def migrate_sqlite(db_path: Path, dry_run: bool = False) -> None:
    """Apply domain migration to a single SQLite database."""
    logger.info("Migrating: %s", db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # sitemap_urls
        _add_column_if_missing(conn, "sitemap_urls", "domain", "TEXT", dry_run)
        if not dry_run:
            domain_expr = _extract_domain_sql_expr()
            conn.execute(f"""
                UPDATE sitemap_urls
                SET domain = ({domain_expr})
                WHERE domain IS NULL AND sitemap_source IS NOT NULL
            """)
            logger.info("Backfilled domain on sitemap_urls")

        # sitemap_freshness
        _add_column_if_missing(conn, "sitemap_freshness", "domain", "TEXT", dry_run)
        if not dry_run:
            domain_expr = _extract_domain_sql_expr()
            conn.execute(f"""
                UPDATE sitemap_freshness
                SET domain = ({domain_expr})
                WHERE domain IS NULL AND sitemap_source IS NOT NULL
            """)
            logger.info("Backfilled domain on sitemap_freshness")

        # url_volume_decay — no backfill possible (no sitemap_source column)
        _add_column_if_missing(conn, "url_volume_decay", "domain", "TEXT", dry_run)
        if not dry_run:
            logger.info(
                "url_volume_decay: domain column added (NULL values). "
                "Re-run SitemapAggregator to repopulate with correct domain values."
            )

        if not dry_run:
            conn.commit()
            logger.info("Migration complete: %s", db_path)

    except Exception as e:
        conn.rollback()
        logger.error("Migration failed: %s — %s", db_path, e)
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--db-path",
        default="data/llm-bot-logs.db",
        help="SQLite database path (glob ok)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show changes without applying"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    db_paths = glob.glob(args.db_path)
    if not db_paths:
        logger.error("No database files found matching: %s", args.db_path)
        sys.exit(1)

    for db_path in db_paths:
        migrate_sqlite(Path(db_path), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
