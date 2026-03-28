#!/usr/bin/env python3
"""
Migration script: add composite unique index to url_performance (v2.1.1 → v2.1.2).

Adds UNIQUE INDEX on url_performance(domain, request_date, url_path) to prevent
duplicate rows from repeated aggregation runs.

Usage:
    python scripts/migrations/migrate_fix_url_performance_unique_key.py --db-path data/llm-bot-logs.db
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

INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_url_performance_natural_key
ON url_performance(domain, request_date, url_path)
"""


def migrate_sqlite(db_path: Path, dry_run: bool = False) -> None:
    logger.info("Migrating: %s", db_path)
    conn = sqlite3.connect(db_path)
    try:
        if dry_run:
            logger.info("[DRY RUN] Would execute: %s", INDEX_SQL.strip())
        else:
            conn.execute(INDEX_SQL)
            conn.commit()
            logger.info("Created idx_url_performance_natural_key on %s", db_path)
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            logger.info("Index already exists — skipping")
        else:
            raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--db-path", default="data/llm-bot-logs.db")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_paths = glob.glob(args.db_path)
    if not db_paths:
        logger.error("No database files found: %s", args.db_path)
        sys.exit(1)

    for db_path in db_paths:
        migrate_sqlite(Path(db_path), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
