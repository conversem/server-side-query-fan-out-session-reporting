#!/usr/bin/env python3
"""
CLI script to run the LLM bot traffic ETL pipeline.

Usage:
    # Run for yesterday with SQLite
    python scripts/run_pipeline.py --daily

    # Run for specific date range
    python scripts/run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-31

    # Dry run (preview without writing)
    python scripts/run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-07 --dry-run

    # Skip session aggregation
    python scripts/run_pipeline.py --daily --skip-sessions

    # Check pipeline status
    python scripts/run_pipeline.py --status
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config import OPTIMAL_WINDOW_MS
from llm_bot_pipeline.pipeline import LocalPipeline, setup_logging
from llm_bot_pipeline.reporting import SessionAggregator
from llm_bot_pipeline.storage import get_backend


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def run_session_aggregation(
    db_path: Path = None,
    start_date: date = None,
    end_date: date = None,
    dry_run: bool = False,
) -> dict:
    """
    Run session aggregation on processed data.

    Queries bot_requests_daily for user_request traffic and creates
    query fan-out sessions using the 100ms time window.

    Args:
        db_path: Path to SQLite database
        start_date: Start date for data to process
        end_date: End date for data to process
        dry_run: If True, skip actual session creation

    Returns:
        Dictionary with session aggregation metrics
    """
    logger = logging.getLogger(__name__)
    logger.info("[Session Aggregation] Starting...")

    result = {
        "success": False,
        "sessions_created": 0,
        "requests_processed": 0,
        "high_confidence": 0,
        "medium_confidence": 0,
        "low_confidence": 0,
        "duration_seconds": 0.0,
        "error": None,
    }

    if dry_run:
        logger.info("[Session Aggregation] DRY RUN - skipping session creation")
        result["success"] = True
        return result

    # Initialize storage backend
    kwargs = {}
    if db_path:
        kwargs["db_path"] = db_path
    backend = get_backend("sqlite", **kwargs)
    backend.initialize()

    try:
        # Query user-request traffic from bot_requests_daily
        logger.info("[Session Aggregation] Fetching user-request traffic...")

        date_filter = ""
        params = {}
        if start_date:
            date_filter += " AND request_date >= :start_date"
            params["start_date"] = start_date.isoformat()
        if end_date:
            date_filter += " AND request_date <= :end_date"
            params["end_date"] = end_date.isoformat()

        query = f"""
            SELECT
                request_timestamp AS datetime,
                url_path AS url,
                bot_provider,
                bot_name
            FROM bot_requests_daily
            WHERE bot_category = 'user_request'
            {date_filter}
            ORDER BY request_timestamp
        """

        rows = backend.query(query, params)
        logger.info(f"[Session Aggregation] Found {len(rows)} user-request records")

        if not rows:
            logger.info("[Session Aggregation] No data to process")
            result["success"] = True
            return result

        # Convert to DataFrame
        df = pd.DataFrame(rows)
        df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")
        result["requests_processed"] = len(df)

        # Delete existing sessions for the date range (to allow reprocessing)
        with SessionAggregator(backend=backend) as aggregator:
            if start_date:
                for day in pd.date_range(start_date, end_date or start_date):
                    aggregator.delete_sessions(session_date=day.strftime("%Y-%m-%d"))

            # Create sessions
            agg_result = aggregator.create_sessions_from_dataframe(
                df=df,
                window_ms=OPTIMAL_WINDOW_MS,
                timestamp_col="datetime",
                url_col="url",
                group_by="bot_provider",
                bot_name_col="bot_name",
            )

            result["success"] = agg_result.success
            result["sessions_created"] = agg_result.sessions_created
            result["high_confidence"] = agg_result.high_confidence_count
            result["medium_confidence"] = agg_result.medium_confidence_count
            result["low_confidence"] = agg_result.low_confidence_count
            result["duration_seconds"] = agg_result.duration_seconds

            if not agg_result.success:
                result["error"] = agg_result.error

        logger.info(
            f"[Session Aggregation] Created {result['sessions_created']} sessions "
            f"in {result['duration_seconds']:.1f}s"
        )

    except Exception as e:
        logger.exception(f"[Session Aggregation] Failed: {e}")
        result["error"] = str(e)

    finally:
        backend.close()

    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run the LLM bot traffic ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run for yesterday
  python scripts/run_pipeline.py --daily

  # Run for specific date range
  python scripts/run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-31

  # Preview without writing (dry run)
  python scripts/run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-07 --dry-run
        """,
    )

    # Backend selection (SQLite only in public version)
    parser.add_argument(
        "--backend",
        choices=["sqlite"],
        default="sqlite",
        help="Storage backend to use (default: sqlite)",
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )

    # Date range
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

    # Mode options
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Run for yesterday only",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run in backfill mode (process in batches)",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=7,
        help="Days per batch in backfill mode (default: 7)",
    )

    # Processing options
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="full",
        help="Processing mode: 'incremental' (append) or 'full' (replace) [default: full]",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview without writing data",
    )
    parser.add_argument(
        "--skip-sessions",
        action="store_true",
        help="Skip session aggregation step after ETL",
    )

    # Optional
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline status and exit",
    )

    args = parser.parse_args()

    # Validate batch_days early
    if args.batch_days < 1:
        parser.error("--batch-days must be >= 1")

    # Setup logging
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)

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

    # Show status and exit
    if args.status:
        status = pipeline.get_pipeline_status()
        print("\n📊 Pipeline Status")
        print("=" * 50)
        print(f"  Backend: sqlite")
        for key, value in status.items():
            print(f"  {key}: {value}")
        print()
        return 0

    # Determine date range
    if args.daily:
        start_date = date.today() - timedelta(days=1)
        end_date = start_date
        logger.info(f"Daily mode: processing {start_date}")
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    elif args.start_date:
        start_date = args.start_date
        end_date = date.today() - timedelta(days=1)
        logger.info(f"Using end date: {end_date} (yesterday)")
    else:
        logger.error("Must specify --daily or --start-date/--end-date")
        parser.print_help()
        return 1

    # Run pipeline
    print()
    print("🚀 LLM Bot Traffic ETL Pipeline")
    print("=" * 50)
    print(f"  Backend: sqlite")
    if args.db_path:
        print(f"  Database: {args.db_path}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Mode: {args.mode}")
    if args.dry_run:
        print("  ⚠️  DRY RUN - no data will be written")
    print()

    try:
        if args.backfill:
            # Backfill mode - runs as single batch for SQLite
            logger.info("Backfill mode runs as single batch for SQLite backend")
            result = pipeline.run(
                start_date=start_date,
                end_date=end_date,
                mode=args.mode,
                dry_run=args.dry_run,
            )
            results = [result]

            # ETL Summary
            successful = sum(1 for r in results if r.success)
            total_rows = sum(r.transformed_rows for r in results)
            total_duration = sum(r.duration_seconds or 0 for r in results)

            print()
            print("📈 ETL Backfill Summary")
            print("=" * 50)
            print(f"  Batches: {successful}/{len(results)} successful")
            print(f"  Total rows: {total_rows:,}")
            print(f"  Total duration: {total_duration:.1f}s")

            # Run session aggregation for entire date range if all ETL batches succeeded
            session_result = None
            if successful == len(results) and not args.skip_sessions:
                print()
                print("🔗 Session Aggregation")
                print("=" * 50)

                session_result = run_session_aggregation(
                    db_path=args.db_path,
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=args.dry_run,
                )

                print(f"  Success: {'✅' if session_result['success'] else '❌'}")
                print(f"  Requests processed: {session_result['requests_processed']:,}")
                print(f"  Sessions created: {session_result['sessions_created']:,}")
                print(
                    f"  Confidence: high={session_result['high_confidence']}, "
                    f"medium={session_result['medium_confidence']}, "
                    f"low={session_result['low_confidence']}"
                )
                print(f"  Duration: {session_result['duration_seconds']:.1f}s")

                if session_result["error"]:
                    print()
                    print(f"❌ Session Error: {session_result['error']}")

            elif args.skip_sessions:
                print()
                print("⏭️  Session aggregation skipped (--skip-sessions)")

            # Determine overall success
            etl_success = successful == len(results)
            session_success = session_result is None or session_result["success"]

            return 0 if (etl_success and session_success) else 1

        else:
            # Single run
            result = pipeline.run(
                start_date=start_date,
                end_date=end_date,
                mode=args.mode,
                dry_run=args.dry_run,
            )

            # Print ETL summary
            print()
            print("📈 ETL Pipeline Result")
            print("=" * 50)
            print(f"  Success: {'✅' if result.success else '❌'}")
            print(f"  Raw rows: {result.raw_rows:,}")
            print(f"  Transformed rows: {result.transformed_rows:,}")
            print(f"  Duplicates removed: {result.duplicates_removed:,}")
            duration = result.duration_seconds or 0
            print(f"  Duration: {duration:.1f}s")

            if result.errors:
                print()
                print("❌ Errors:")
                for error in result.errors:
                    print(f"  - {error}")

            # Run session aggregation if ETL succeeded and not skipped
            session_result = None
            if result.success and not args.skip_sessions:
                print()
                print("🔗 Session Aggregation")
                print("=" * 50)

                session_result = run_session_aggregation(
                    db_path=args.db_path,
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=args.dry_run,
                )

                print(f"  Success: {'✅' if session_result['success'] else '❌'}")
                print(f"  Requests processed: {session_result['requests_processed']:,}")
                print(f"  Sessions created: {session_result['sessions_created']:,}")
                print(
                    f"  Confidence: high={session_result['high_confidence']}, "
                    f"medium={session_result['medium_confidence']}, "
                    f"low={session_result['low_confidence']}"
                )
                print(f"  Duration: {session_result['duration_seconds']:.1f}s")

                if session_result["error"]:
                    print()
                    print(f"❌ Session Error: {session_result['error']}")

            elif args.skip_sessions:
                print()
                print("⏭️  Session aggregation skipped (--skip-sessions)")

            # Determine overall success
            pipeline_success = result.success
            if session_result and not session_result["success"]:
                pipeline_success = False

            return 0 if pipeline_success else 1

    finally:
        # Cleanup: close pipeline
        if hasattr(pipeline, "close"):
            pipeline.close()


if __name__ == "__main__":
    sys.exit(main())
