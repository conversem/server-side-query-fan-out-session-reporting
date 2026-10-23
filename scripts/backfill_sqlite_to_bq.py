#!/usr/bin/env python3
"""
Backfill BigQuery from a legacy/POC SQLite database.

Migrates historical data that cannot be re-fetched from Cloudflare (>7-day window)
into the production BigQuery dataset. Handles schema differences between POC and
current pipeline versions.

Usage:
    # Dry-run (validate without writing)
    python scripts/backfill_sqlite_to_bq.py \
        --sqlite-path /data/example.db \
        --domain example.com \
        --dry-run

    # Full migration
    python scripts/backfill_sqlite_to_bq.py \
        --sqlite-path /data/example.db \
        --domain example.com

    # Specific tables and date range
    python scripts/backfill_sqlite_to_bq.py \
        --sqlite-path /data/example.db \
        --domain example.com \
        --tables raw_bot_requests,bot_requests_daily \
        --start-date 2026-02-10 \
        --end-date 2026-02-20

    # Force re-insert (delete existing BQ data in range first)
    python scripts/backfill_sqlite_to_bq.py \
        --sqlite-path /data/example.db \
        --domain example.com \
        --force
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from llm_bot_pipeline.pipeline.backfill import (
    MIGRATION_ORDER,
    SQLiteBackfillManager,
)

logger = logging.getLogger("backfill_sqlite_to_bq")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill BigQuery from a legacy/POC SQLite database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sqlite-path",
        type=Path,
        required=True,
        help="Path to the source SQLite database file.",
    )
    parser.add_argument(
        "--domain",
        type=str,
        required=True,
        help="Domain label to inject (e.g. example.com).",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Comma-separated list of tables to migrate (default: all populated).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per BigQuery insert batch (default: 1000).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date filter (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date filter (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and log without writing to BigQuery.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing BQ data in date range before inserting.",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=None,
        help="Path to checkpoint JSON file (default: data/backfill_checkpoint.json).",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default=None,
        help="GCP project ID (default: from settings/env).",
    )
    parser.add_argument(
        "--credentials-path",
        type=Path,
        default=None,
        help="Path to GCP service account JSON key (default: ADC or settings).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def create_bq_backend(project_id: str | None, credentials_path: Path | None):
    """Create a BigQuery backend, using settings as fallback for params."""
    from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend

    if not project_id:
        try:
            from llm_bot_pipeline.config.settings import get_settings

            settings = get_settings()
            project_id = settings.gcp_project_id
            if not credentials_path and settings.service_account_key_path.exists():
                credentials_path = settings.service_account_key_path
        except Exception:
            pass

    if not project_id:
        project_id = _project_from_env()

    if not project_id:
        print("ERROR: --project-id is required (or set GCP_PROJECT_ID env var)")
        sys.exit(1)

    creds = (
        str(credentials_path)
        if credentials_path and credentials_path.exists()
        else None
    )
    backend = BigQueryBackend(project_id=project_id, credentials_path=creds)
    backend.initialize()
    return backend


def _project_from_env() -> str | None:
    import os

    return os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    if not args.sqlite_path.exists():
        print(f"ERROR: SQLite file not found: {args.sqlite_path}")
        sys.exit(1)

    tables = args.tables.split(",") if args.tables else None
    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None

    if tables:
        invalid = [t for t in tables if t not in MIGRATION_ORDER]
        if invalid:
            print(f"ERROR: Unknown tables: {invalid}")
            print(f"Valid tables: {MIGRATION_ORDER}")
            sys.exit(1)

    # Print source database info
    print(f"\nSource:  {args.sqlite_path}")
    print(f"Domain:  {args.domain}")
    print(f"Mode:    {'DRY-RUN' if args.dry_run else 'LIVE'}")
    if args.force:
        print("Force:   YES (will delete existing BQ data in range)")
    print()

    bq = create_bq_backend(args.project_id, args.credentials_path)

    try:
        mgr = SQLiteBackfillManager(
            sqlite_path=args.sqlite_path,
            domain=args.domain,
            bq_backend=bq,
            batch_size=args.batch_size,
            checkpoint_path=args.checkpoint_path,
        )

        # Show source info
        schema_ver = mgr.detect_schema_version()
        available = mgr.get_tables()
        print(f"Schema version: {schema_ver}")
        print(f"Populated tables: {available}")
        for t in available:
            count = mgr.get_table_row_count(t)
            mn, mx = mgr.get_date_range(t)
            date_info = f"  ({mn} .. {mx})" if mn else ""
            print(f"  {t:<30s} {count:>8,} rows{date_info}")
        print()

        results = mgr.migrate_all(
            tables=tables,
            start_date=start_date,
            end_date=end_date,
            dry_run=args.dry_run,
            force=args.force,
        )

        mgr.print_summary(results)

        failed = [t for t, r in results.items() if not r.success]
        if failed:
            print(f"FAILED tables: {failed}")
            sys.exit(1)

    finally:
        mgr.close()
        bq.close()


if __name__ == "__main__":
    main()
