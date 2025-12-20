#!/usr/bin/env python3
"""
CLI script to ingest Cloudflare logs into the local SQLite database.

Usage:
    # Ingest last 3 days of logs
    python scripts/ingest_logs.py --days 3

    # Ingest specific date range
    python scripts/ingest_logs.py --start-date 2024-01-01 --end-date 2024-01-31

    # Resume interrupted ingestion
    python scripts/ingest_logs.py --days 7 --resume

    # Dry run (preview without writing)
    python scripts/ingest_logs.py --days 3 --dry-run
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.pipeline import LocalPipeline, setup_logging
from llm_bot_pipeline.storage import get_backend


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def ingest_date_range(
    pipeline: LocalPipeline,
    start_date: date,
    end_date: date,
    resume: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Ingest logs for a date range.

    Args:
        pipeline: Initialized pipeline
        start_date: Start date
        end_date: End date
        resume: If True, skip dates that already have data
        dry_run: If True, don't write data

    Returns:
        Dictionary with ingestion metrics
    """
    logger = logging.getLogger(__name__)

    results = {
        "dates_processed": 0,
        "dates_skipped": 0,
        "total_records": 0,
        "errors": [],
    }

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()

        # Check if we should skip this date
        if resume:
            existing = pipeline.backend.query(
                "SELECT COUNT(*) as count FROM bot_requests_daily WHERE request_date = :date",
                {"date": date_str},
            )
            if existing and existing[0]["count"] > 0:
                logger.info(
                    f"Skipping {date_str} - already has {existing[0]['count']} records"
                )
                results["dates_skipped"] += 1
                current_date += timedelta(days=1)
                continue

        logger.info(f"Processing {date_str}...")

        try:
            if dry_run:
                logger.info(f"  [DRY RUN] Would ingest {date_str}")
            else:
                result = pipeline.run(
                    start_date=current_date,
                    end_date=current_date,
                    mode="full",
                    dry_run=False,
                )

                if result.success:
                    results["dates_processed"] += 1
                    results["total_records"] += result.transformed_rows
                    logger.info(f"  Ingested {result.transformed_rows} records")
                else:
                    results["errors"].append(f"{date_str}: {result.errors}")

        except Exception as e:
            logger.error(f"Error processing {date_str}: {e}")
            results["errors"].append(f"{date_str}: {str(e)}")

        current_date += timedelta(days=1)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest Cloudflare logs into SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest last 3 days of logs
  python scripts/ingest_logs.py --days 3

  # Ingest specific date range
  python scripts/ingest_logs.py --start-date 2024-01-01 --end-date 2024-01-31

  # Resume interrupted ingestion
  python scripts/ingest_logs.py --days 7 --resume

  # Dry run (preview without writing)
  python scripts/ingest_logs.py --days 3 --dry-run
        """,
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to ingest (from today)",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip dates that already have data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing data",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)

    # Determine date range
    if args.days:
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    elif args.start_date:
        start_date = args.start_date
        end_date = date.today() - timedelta(days=1)
    else:
        logger.error("Must specify --days or --start-date/--end-date")
        parser.print_help()
        return 1

    # Initialize pipeline
    try:
        pipeline = LocalPipeline(
            backend_type="sqlite",
            db_path=args.db_path,
        )
        pipeline.initialize()
    except Exception as e:
        logger.error(f"Failed to initialize pipeline: {e}")
        return 1

    print()
    print("📥 Ingesting Cloudflare Logs")
    print("=" * 50)
    print(f"  Backend: sqlite")
    if args.db_path:
        print(f"  Database: {args.db_path}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Resume mode: {'Yes' if args.resume else 'No'}")
    if args.dry_run:
        print("  ⚠️  DRY RUN - no data will be written")
    print()

    try:
        results = ingest_date_range(
            pipeline=pipeline,
            start_date=start_date,
            end_date=end_date,
            resume=args.resume,
            dry_run=args.dry_run,
        )

        print()
        print("📊 Ingestion Summary")
        print("=" * 50)
        print(f"  Dates processed: {results['dates_processed']}")
        print(f"  Dates skipped: {results['dates_skipped']}")
        print(f"  Total records: {results['total_records']:,}")

        if results["errors"]:
            print()
            print("❌ Errors:")
            for error in results["errors"]:
                print(f"  - {error}")
            return 1

        return 0

    finally:
        if hasattr(pipeline, "close"):
            pipeline.close()


if __name__ == "__main__":
    sys.exit(main())
