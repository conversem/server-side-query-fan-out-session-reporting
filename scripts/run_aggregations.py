#!/usr/bin/env python3
"""
CLI script to run reporting aggregations on processed data.

Usage:
    # Run all aggregations for yesterday
    python scripts/run_aggregations.py --daily

    # Run for specific date range
    python scripts/run_aggregations.py --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.pipeline import setup_logging
from llm_bot_pipeline.reporting import LocalReportingAggregator
from llm_bot_pipeline.storage import get_backend


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run reporting aggregations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all aggregations for yesterday
  python scripts/run_aggregations.py --daily

  # Run for specific date range
  python scripts/run_aggregations.py --start-date 2024-01-01 --end-date 2024-01-31
        """,
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
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
        "--daily",
        action="store_true",
        help="Run for yesterday only",
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
    if args.daily:
        start_date = date.today() - timedelta(days=1)
        end_date = start_date
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        logger.error("Must specify --daily or --start-date/--end-date")
        parser.print_help()
        return 1

    # Initialize backend
    kwargs = {}
    if args.db_path:
        kwargs["db_path"] = args.db_path
    backend = get_backend("sqlite", **kwargs)
    backend.initialize()

    print()
    print("📊 Running Aggregations")
    print("=" * 50)
    print(f"  Backend: sqlite")
    if args.db_path:
        print(f"  Database: {args.db_path}")
    print(f"  Date range: {start_date} to {end_date}")
    print()

    try:
        aggregator = LocalReportingAggregator(backend=backend)
        result = aggregator.run_all(start_date=start_date, end_date=end_date)

        print()
        print("📈 Aggregation Results")
        print("=" * 50)
        print(f"  Success: {'✅' if result.success else '❌'}")
        print(f"  Records processed: {result.records_processed:,}")
        print(f"  Duration: {result.duration_seconds:.1f}s")

        if result.errors:
            print()
            print("❌ Errors:")
            for error in result.errors:
                print(f"  - {error}")
            return 1

        return 0

    finally:
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
