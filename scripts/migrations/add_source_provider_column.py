#!/usr/bin/env python3
"""
Migration script to add source_provider column to raw_bot_requests table.

This migration adds data provenance tracking by adding a source_provider column
that stores which provider (universal, cloudflare, aws_cloudfront, etc.) the
record was ingested from.

Usage:
    python scripts/migrations/add_source_provider_column.py [--db-path PATH]

Options:
    --db-path PATH    Path to SQLite database file (default: data/llm-bot-logs.db)
    --dry-run         Show what would be done without making changes
    --verbose         Enable verbose output
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def column_exists(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    return column in columns


def run_migration(db_path: Path, dry_run: bool = False) -> bool:
    """
    Run the migration to add source_provider column.

    Args:
        db_path: Path to the SQLite database
        dry_run: If True, only show what would be done

    Returns:
        True if migration was successful or already applied
    """
    logger.info(f"Running migration on database: {db_path}")

    if not db_path.exists():
        logger.warning(f"Database file does not exist: {db_path}")
        logger.info("Column will be added when database is created.")
        return True

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Check if column already exists
        if column_exists(cursor, "raw_bot_requests", "source_provider"):
            logger.info(
                "Column 'source_provider' already exists. Migration not needed."
            )
            conn.close()
            return True

        # Add the column
        migration_sql = """
        ALTER TABLE raw_bot_requests
        ADD COLUMN source_provider TEXT
        """

        if dry_run:
            logger.info("DRY RUN: Would execute:")
            logger.info(f"  {migration_sql.strip()}")
        else:
            logger.info("Adding source_provider column...")
            cursor.execute(migration_sql)
            conn.commit()
            logger.info("Migration completed successfully.")

        # Update existing records with default value
        update_sql = """
        UPDATE raw_bot_requests
        SET source_provider = 'unknown'
        WHERE source_provider IS NULL
        """

        if dry_run:
            logger.info("DRY RUN: Would execute:")
            logger.info(f"  {update_sql.strip()}")
        else:
            logger.info("Setting default value for existing records...")
            cursor.execute(update_sql)
            affected_rows = cursor.rowcount
            conn.commit()
            logger.info(f"Updated {affected_rows} existing records.")

        conn.close()
        return True

    except sqlite3.Error as e:
        logger.error(f"SQLite error during migration: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during migration: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Add source_provider column to raw_bot_requests table"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/llm-bot-logs.db"),
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    success = run_migration(args.db_path, dry_run=args.dry_run)

    if success:
        logger.info("Migration completed successfully.")
        return 0
    else:
        logger.error("Migration failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

