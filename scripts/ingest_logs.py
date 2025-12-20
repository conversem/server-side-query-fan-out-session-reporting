#!/usr/bin/env python3
"""
CLI script to ingest Cloudflare logs into local storage.

Pulls logs from Cloudflare's Logpull API and inserts them into the storage backend.
Primarily used for local development with SQLite, but supports SQLite as well.

Usage:
    # Ingest last 7 days to SQLite (default)
    python scripts/ingest_logs.py --days 7

    # Ingest specific date range
    python scripts/ingest_logs.py --start-date 2024-01-01 --end-date 2024-01-07

    # Ingest last 3 days
    python scripts/ingest_logs.py --days 3

    # Dry run (estimate only, no ingestion)
    python scripts/ingest_logs.py --days 7 --dry-run

    # Resume interrupted ingestion (skip existing dates)
    python scripts/ingest_logs.py --days 7 --resume

    # Show available date range
    python scripts/ingest_logs.py --available
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.cloudflare import (
    IngestionResult,
    estimate_log_volume,
    get_available_date_range,
    ingest_to_sqlite,
    pull_logs_for_date_range,
)
from llm_bot_pipeline.config.settings import Settings, get_settings
from llm_bot_pipeline.pipeline.orchestrator import setup_logging
from llm_bot_pipeline.storage import get_backend


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def validate_credentials(settings: Settings) -> list[str]:
    """Validate that required credentials are configured."""
    errors = []

    if not settings.cloudflare_api_token:
        errors.append("Missing cloudflare.api_token (required for Logpull API)")

    if not settings.cloudflare_zone_id:
        errors.append("Missing cloudflare.zone_id (required for Logpull API)")

    return errors


def get_existing_dates(
    backend: "StorageBackend", table_name: str = "raw_bot_requests"
) -> set[date]:
    """Get set of dates that already have data in the backend.

    Args:
        backend: Storage backend instance
        table_name: Table to check for existing dates

    Returns:
        Set of dates that have existing data
    """
    from llm_bot_pipeline.storage import StorageBackend  # noqa: F811

    if not backend.table_exists(table_name):
        return set()

    try:
        # Use backend-appropriate SQL syntax
        if backend.backend_type == "sqlite":
            sql = """
                SELECT DISTINCT date(EdgeStartTimestamp) as request_date
                FROM raw_bot_requests
            """
        else:
            # For SQLite, use the query method which handles table IDs internally
            # We query from the raw dataset's bot_requests table
            sql = """
                SELECT DISTINCT DATE(EdgeStartTimestamp) as request_date
                FROM `raw_bot_requests`
            """

        results = backend.query(sql)
        dates = set()
        for row in results:
            rd = row.get("request_date")
            if rd:
                if isinstance(rd, str):
                    dates.add(datetime.strptime(rd, "%Y-%m-%d").date())
                elif isinstance(rd, date):
                    dates.add(rd)
        return dates

    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not get existing dates: {e}")
        return set()


def ingest_with_progress(
    start_date: date,
    end_date: date,
    backend_type: str,
    settings: Settings,
    db_path: Optional[Path] = None,
    batch_size: int = 1000,
    skip_dates: Optional[set[date]] = None,
    filter_verified_bots: bool = True,
) -> IngestionResult:
    """
    Ingest logs with progress reporting.

    Args:
        start_date: Start date
        end_date: End date (inclusive)
        backend_type: 'sqlite' or 'sqlite'
        settings: Application settings
        db_path: Path to SQLite database (for sqlite backend)
        batch_size: Records per batch
        skip_dates: Dates to skip (for resume mode)
        filter_verified_bots: If True, only ingest verified bot traffic

    Returns:
        Aggregated IngestionResult
    """
    logger = logging.getLogger(__name__)

    if skip_dates is None:
        skip_dates = set()

    # Calculate total days
    total_days = (end_date - start_date).days + 1
    days_to_process = [
        start_date + timedelta(days=i)
        for i in range(total_days)
        if (start_date + timedelta(days=i)) not in skip_dates
    ]

    if len(days_to_process) == 0:
        logger.info("No dates to process (all skipped)")
        return IngestionResult(
            success=True,
            records_ingested=0,
            start_date=start_date,
            end_date=end_date,
        )

    skipped_count = total_days - len(days_to_process)
    logger.info(
        f"Processing {len(days_to_process)} days"
        + (f" (skipping {skipped_count} existing)" if skipped_count > 0 else "")
    )

    # Aggregate results
    total_ingested = 0
    total_failed = 0
    total_chunks = 0
    start_time_total = time.time()
    errors = []

    # For SQLite without skip_dates, use existing optimized function
    if backend_type == "sqlite" and not skip_dates:
        result = ingest_to_sqlite(
            start_date=start_date,
            end_date=end_date,
            db_path=db_path,
            settings=settings,
            batch_size=batch_size,
            filter_verified_bots=filter_verified_bots,
        )
        return result

    # For SQLite or SQLite with resume mode, process day by day
    backend = get_backend(
        backend_type,
        db_path=db_path if backend_type == "sqlite" else None,
    )
    backend.initialize()

    try:
        for i, current_date in enumerate(days_to_process):
            progress = (i + 1) / len(days_to_process) * 100
            logger.info(
                f"[{progress:5.1f}%] Processing {current_date} "
                f"({i + 1}/{len(days_to_process)})"
            )

            try:
                # Pull logs for this day
                records = list(
                    pull_logs_for_date_range(
                        start_date=current_date,
                        end_date=current_date,
                        settings=settings,
                        filter_verified_bots=filter_verified_bots,
                    )
                )

                if records:
                    # Insert in batches
                    for j in range(0, len(records), batch_size):
                        batch = records[j : j + batch_size]
                        inserted = backend.insert_raw_records(batch)
                        total_ingested += inserted
                        total_chunks += 1

                logger.debug(f"Ingested {len(records)} records for {current_date}")

            except Exception as e:
                logger.error(f"Failed to process {current_date}: {e}")
                errors.append(f"{current_date}: {e}")
                total_failed += 1

    finally:
        backend.close()

    duration = time.time() - start_time_total

    return IngestionResult(
        success=len(errors) == 0,
        records_ingested=total_ingested,
        records_failed=total_failed,
        start_date=start_date,
        end_date=end_date,
        error="; ".join(errors) if errors else None,
        duration_seconds=duration,
        chunks_processed=total_chunks,
    )


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest Cloudflare logs into local storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest last 7 days to SQLite
  python scripts/ingest_logs.py --days 7

  # Ingest specific date range
  python scripts/ingest_logs.py --start-date 2024-12-10 --end-date 2024-12-15

  # Ingest 3 days
  python scripts/ingest_logs.py --days 3

  # Dry run (estimate only)
  python scripts/ingest_logs.py --days 7 --dry-run

  # Resume interrupted ingestion
  python scripts/ingest_logs.py --days 7 --resume

  # Show available date range
  python scripts/ingest_logs.py --available
        """,
    )

    # Backend selection
    parser.add_argument(
        "--backend",
        choices=["sqlite", "sqlite"],
        default="sqlite",
        help="Storage backend to use (default: sqlite)",
    )

    # Date range options
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to ingest (from today backwards)",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date (YYYY-MM-DD, inclusive)",
    )

    # Modes
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate volume only, don't ingest",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip dates that already have data",
    )
    parser.add_argument(
        "--available",
        action="store_true",
        help="Show available date range and exit",
    )

    # Backend-specific options
    parser.add_argument(
    )
    )

    # Processing options
    )
    )

    # Logging

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)

    # Show available date range
    if args.available:
        earliest, latest = get_available_date_range()
        print()
        print("📅 Available Date Range (Cloudflare Logpull)")
        print("=" * 50)
        print(f"  Earliest: {earliest}")
        print(f"  Latest:   {latest}")
        print(f"  Days:     {(latest - earliest).days + 1}")
        print()
        print("Note: Cloudflare retains logs for 7 days only.")
        print()
        return 0

    # Load settings
    try:
        settings = get_settings()
    except Exception as e:
        logger.error(f"Failed to load settings: {e}")
        return 1

    # Validate credentials
    cred_errors = validate_credentials(settings)
    if cred_errors:
        print()
        print("❌ Missing Credentials")
        print("=" * 50)
        for error in cred_errors:
            print(f"  - {error}")
        print()
        print("Configure these in config.enc.yaml or environment variables.")
        print()
        return 1


    # Determine date range
    earliest_available, latest_available = get_available_date_range()

    if args.days:
        end_date = latest_available
        start_date = end_date - timedelta(days=args.days - 1)
        # Clamp to available range
        if start_date < earliest_available:
            logger.warning(
                f"Start date {start_date} exceeds retention. "
                f"Using {earliest_available} instead."
            )
            start_date = earliest_available
    elif args.start_date:
        start_date = args.start_date
        end_date = args.end_date or latest_available

        # Validate against retention
        if start_date < earliest_available:
            logger.error(
                f"Start date {start_date} exceeds 7-day retention limit. "
                f"Earliest available: {earliest_available}"
            )
            return 1
    else:
        logger.error("Must specify --days, --start-date, or --available")
        parser.print_help()
        return 1

    # Validate date order
    if start_date > end_date:
        logger.error(f"Start date ({start_date}) must be <= end date ({end_date})")
        return 1

    if end_date > latest_available:
        logger.warning(
            f"End date {end_date} is in the future. Using {latest_available}."
        )
        end_date = latest_available

    # Print header
    print()
    print("📥 Cloudflare Log Ingestion")
    print("=" * 50)
    print(f"  Backend:    {args.backend}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Days:       {(end_date - start_date).days + 1}")
    if args.backend == "sqlite":
        db_path = args.db_path or Path(settings.sqlite_db_path)
        print(f"  Database:   {db_path}")
    if args.dry_run:
        print("  ⚠️  DRY RUN - no data will be ingested")
    if args.resume:
        print("  ♻️  RESUME - skipping existing dates")
    print()

    # Dry run: estimate and exit
    if args.dry_run:
        estimate = estimate_log_volume(start_date, end_date)
        print("📊 Volume Estimate")
        print("=" * 50)
        print(f"  Time range:          {estimate['total_hours']:.0f} hours")
        print(f"  API calls needed:    {estimate['api_calls_needed']}")
        print(f"  Rate limit:          {estimate['rate_limit_per_minute']}/min")
        print(
            f"  Estimated duration:  {estimate['estimated_time_minutes']:.1f} minutes"
        )
        print()
        print("Note: Actual record count depends on traffic volume.")
        print()
        return 0

    # Check for existing data in resume mode
    skip_dates: set[date] = set()
    if args.resume:
        logger.info("Checking for existing data...")
        try:
            backend = get_backend(
                args.backend,
                db_path=args.db_path if args.backend == "sqlite" else None,
            )
            backend.initialize()
            skip_dates = get_existing_dates(backend)
            backend.close()

            if skip_dates:
                logger.info(f"Found {len(skip_dates)} dates with existing data")
        except Exception as e:
            logger.warning(f"Could not check existing data: {e}")

    # Run ingestion
    filter_verified = not args.include_unverified
    if not filter_verified:
        logger.info("Including unverified bot traffic")

    logger.info("Starting ingestion...")

    result = ingest_with_progress(
        start_date=start_date,
        end_date=end_date,
        backend_type=args.backend,
        settings=settings,
        db_path=args.db_path,
        batch_size=args.batch_size,
        skip_dates=skip_dates if args.resume else None,
        filter_verified_bots=filter_verified,
    )

    # Print results
    print()
    print("📋 Results")
    print("=" * 50)
    status = "✅" if result.success else "❌"
    print(f"  Status:           {status}")
    print(f"  Records ingested: {result.records_ingested:,}")
    if result.records_failed:
        print(f"  Records failed:   {result.records_failed:,}")
    print(f"  Batches:          {result.chunks_processed}")
    print(f"  Duration:         {result.duration_seconds:.1f}s")

    if result.records_ingested > 0 and result.duration_seconds > 0:
        rate = result.records_ingested / result.duration_seconds
        print(f"  Rate:             {rate:.0f} records/sec")

    if result.error:
        print()
        print("❌ Errors:")
        print(f"  {result.error}")

    print()

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())

