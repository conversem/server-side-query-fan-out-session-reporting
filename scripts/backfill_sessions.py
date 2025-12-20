#!/usr/bin/env python3
"""
Backfill query fan-out sessions from historical request data.

Processes existing bot_requests_daily data and creates query fan-out sessions
retroactively. Supports batch processing, progress tracking, and resume capability.

Usage:
    # Backfill sessions for a date range
    python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31

    # Backfill with batch size (days per batch)
    python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --batch-days 7

    # Resume interrupted backfill (skip dates with existing sessions)
    python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --resume

    # Force reprocess (delete and recreate existing sessions)
    python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --force

    # Dry run (preview without creating sessions)
    python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --dry-run
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config import OPTIMAL_WINDOW_MS
from llm_bot_pipeline.pipeline import setup_logging
from llm_bot_pipeline.reporting import SessionAggregator
from llm_bot_pipeline.storage import get_backend

logger = logging.getLogger(__name__)


@dataclass
class BackfillResult:
    """Result of a backfill operation."""

    success: bool
    start_date: date
    end_date: date
    days_processed: int = 0
    days_skipped: int = 0
    total_sessions_created: int = 0
    total_requests_processed: int = 0
    high_confidence_count: int = 0
    medium_confidence_count: int = 0
    low_confidence_count: int = 0
    duration_seconds: float = 0.0
    errors: list = field(default_factory=list)


@dataclass
class DayResult:
    """Result of processing a single day."""

    date: date
    success: bool
    sessions_created: int = 0
    requests_processed: int = 0
    high_confidence: int = 0
    medium_confidence: int = 0
    low_confidence: int = 0
    skipped: bool = False
    error: Optional[str] = None


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def get_dates_with_sessions(backend, start_date: date, end_date: date) -> set:
    """
    Get set of dates that already have sessions.

    Args:
        backend: Storage backend
        start_date: Start of date range
        end_date: End of date range

    Returns:
        Set of date strings (YYYY-MM-DD) with existing sessions
    """
    query = """
        SELECT DISTINCT session_date
        FROM query_fanout_sessions
        WHERE session_date >= :start_date
          AND session_date <= :end_date
    """
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        rows = backend.query(query, params)
        return {row["session_date"] for row in rows}
    except Exception:
        # Table might not exist yet
        return set()


def get_dates_with_data(backend, start_date: date, end_date: date) -> set:
    """
    Get set of dates that have user_request data to process.

    Args:
        backend: Storage backend
        start_date: Start of date range
        end_date: End of date range

    Returns:
        Set of date strings (YYYY-MM-DD) with user_request data
    """
    query = """
        SELECT DISTINCT request_date
        FROM bot_requests_daily
        WHERE bot_category = 'user_request'
          AND request_date >= :start_date
          AND request_date <= :end_date
    """
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        rows = backend.query(query, params)
        return {row["request_date"] for row in rows}
    except Exception:
        return set()


def process_single_day(
    backend,
    aggregator: SessionAggregator,
    target_date: date,
    force: bool = False,
    dry_run: bool = False,
) -> DayResult:
    """
    Process a single day's data into sessions.

    Args:
        backend: Storage backend
        aggregator: SessionAggregator instance
        target_date: Date to process
        force: If True, delete existing sessions before creating
        dry_run: If True, skip actual session creation

    Returns:
        DayResult with processing statistics
    """
    date_str = target_date.isoformat()
    result = DayResult(date=target_date, success=False)

    if dry_run:
        result.success = True
        result.skipped = True
        return result

    try:
        # Delete existing sessions if force mode
        if force:
            aggregator.delete_sessions(session_date=date_str)

        # Query user-request data for this day
        query = """
            SELECT
                request_timestamp AS datetime,
                url_path AS url,
                bot_provider,
                bot_name
            FROM bot_requests_daily
            WHERE bot_category = 'user_request'
              AND request_date = :request_date
            ORDER BY request_timestamp
        """
        rows = backend.query(query, {"request_date": date_str})

        if not rows:
            result.success = True
            result.requests_processed = 0
            return result

        # Convert to DataFrame
        df = pd.DataFrame(rows)
        df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
        result.requests_processed = len(df)

        # Create sessions
        agg_result = aggregator.create_sessions_from_dataframe(
            df=df,
            window_ms=OPTIMAL_WINDOW_MS,
            timestamp_col="datetime",
            url_col="url",
            group_by="bot_provider",
            bot_name_col="bot_name",
        )

        result.success = agg_result.success
        result.sessions_created = agg_result.sessions_created
        result.high_confidence = agg_result.high_confidence_count
        result.medium_confidence = agg_result.medium_confidence_count
        result.low_confidence = agg_result.low_confidence_count

        if not agg_result.success:
            result.error = agg_result.error

    except Exception as e:
        result.error = str(e)
        logger.exception(f"Failed to process {date_str}: {e}")

    return result


def run_backfill(
    backend_type: str = "sqlite",
    db_path: Optional[Path] = None,
    start_date: date = None,
    end_date: date = None,
    batch_days: int = 1,
    resume: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> BackfillResult:
    """
    Run backfill for a date range.

    Args:
        backend_type: Storage backend type
        db_path: Path to SQLite database
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        batch_days: Days to process per batch (for progress reporting)
        resume: If True, skip dates with existing sessions
        force: If True, delete and recreate existing sessions
        dry_run: If True, preview without creating sessions

    Returns:
        BackfillResult with overall statistics
    """
    started_at = datetime.now()

    result = BackfillResult(
        success=False,
        start_date=start_date,
        end_date=end_date,
    )

    # Initialize backend
    kwargs = {}
    if backend_type == "sqlite" and db_path:
        kwargs["db_path"] = db_path
    backend = get_backend(backend_type, **kwargs)
    backend.initialize()

    try:
        # Get all dates in range
        all_dates = pd.date_range(start_date, end_date).date.tolist()
        total_days = len(all_dates)

        # Get dates with existing sessions (for resume mode)
        existing_sessions = set()
        if resume:
            existing_sessions = get_dates_with_sessions(backend, start_date, end_date)
            logger.info(f"Found {len(existing_sessions)} dates with existing sessions")

        # Get dates with data to process
        dates_with_data = get_dates_with_data(backend, start_date, end_date)
        logger.info(f"Found {len(dates_with_data)} dates with user_request data")

        # Initialize aggregator
        with SessionAggregator(backend=backend) as aggregator:
            processed = 0
            skipped = 0

            print(f"\n📅 Processing {total_days} days...")
            print("-" * 60)

            for i, target_date in enumerate(all_dates):
                date_str = target_date.isoformat()

                # Skip if no data for this date
                if date_str not in dates_with_data:
                    skipped += 1
                    continue

                # Skip if resume mode and sessions exist
                if resume and date_str in existing_sessions:
                    skipped += 1
                    result.days_skipped += 1
                    print(f"  ⏭️  {date_str}: Skipped (sessions exist)")
                    continue

                # Process the day
                day_result = process_single_day(
                    backend=backend,
                    aggregator=aggregator,
                    target_date=target_date,
                    force=force,
                    dry_run=dry_run,
                )

                if day_result.success:
                    processed += 1
                    result.days_processed += 1
                    result.total_sessions_created += day_result.sessions_created
                    result.total_requests_processed += day_result.requests_processed
                    result.high_confidence_count += day_result.high_confidence
                    result.medium_confidence_count += day_result.medium_confidence
                    result.low_confidence_count += day_result.low_confidence

                    status = (
                        "DRY RUN"
                        if dry_run
                        else f"{day_result.sessions_created} sessions"
                    )
                    print(
                        f"  ✅ {date_str}: {day_result.requests_processed} requests → {status}"
                    )
                else:
                    result.errors.append(f"{date_str}: {day_result.error}")
                    print(f"  ❌ {date_str}: {day_result.error}")

                # Progress update every batch_days
                if (i + 1) % batch_days == 0:
                    pct = ((i + 1) / total_days) * 100
                    print(f"\n  📊 Progress: {i + 1}/{total_days} days ({pct:.0f}%)\n")

        result.success = len(result.errors) == 0

    except Exception as e:
        logger.exception(f"Backfill failed: {e}")
        result.errors.append(str(e))

    finally:
        backend.close()

    result.duration_seconds = (datetime.now() - started_at).total_seconds()
    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill query fan-out sessions from historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill sessions for a date range
  python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31

  # Backfill with progress updates every 7 days
  python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --batch-days 7

  # Resume interrupted backfill (skip dates with existing sessions)
  python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --resume

  # Force reprocess (delete and recreate existing sessions)
  python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --force

  # Dry run (preview without creating sessions)
  python scripts/backfill_sessions.py --start-date 2025-01-01 --end-date 2025-01-31 --dry-run
        """,
    )

    # Required date range
    parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
        help="End date (YYYY-MM-DD, inclusive)",
    )

    # Processing options
    parser.add_argument(
        "--batch-days",
        type=int,
        default=7,
        help="Days between progress updates (default: 7)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip dates with existing sessions (for interrupted runs)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and recreate existing sessions",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without creating sessions",
    )

    # Backend options
    parser.add_argument(
        "--backend",
        choices=["sqlite", "sqlite"],
        default="sqlite",
        help="Storage backend (default: sqlite)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database",
    )

    # Logging
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.start_date > args.end_date:
        parser.error("--start-date must be <= --end-date")

    if args.batch_days < 1:
        parser.error("--batch-days must be >= 1")

    if args.resume and args.force:
        parser.error("Cannot use both --resume and --force")

    # Setup logging
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    # Print banner
    print()
    print("🔄 Session Backfill")
    print("=" * 60)
    print(f"  Date range: {args.start_date} to {args.end_date}")
    print(f"  Backend: {args.backend}")
    if args.db_path:
        print(f"  Database: {args.db_path}")
    print(f"  Mode: {'RESUME' if args.resume else 'FORCE' if args.force else 'NORMAL'}")
    if args.dry_run:
        print("  ⚠️  DRY RUN - no sessions will be created")
    print()

    # Run backfill
    result = run_backfill(
        backend_type=args.backend,
        db_path=args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_days=args.batch_days,
        resume=args.resume,
        force=args.force,
        dry_run=args.dry_run,
    )

    # Print summary
    print()
    print("=" * 60)
    print("📊 Backfill Summary")
    print("=" * 60)
    print(f"  Success: {'✅' if result.success else '❌'}")
    print(f"  Days processed: {result.days_processed}")
    print(f"  Days skipped: {result.days_skipped}")
    print(f"  Requests processed: {result.total_requests_processed:,}")
    print(f"  Sessions created: {result.total_sessions_created:,}")
    print(
        f"  Confidence: high={result.high_confidence_count}, "
        f"medium={result.medium_confidence_count}, "
        f"low={result.low_confidence_count}"
    )
    print(f"  Duration: {result.duration_seconds:.1f}s")

    if result.errors:
        print()
        print("❌ Errors:")
        for error in result.errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(result.errors) > 10:
            print(f"  ... and {len(result.errors) - 10} more errors")

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
