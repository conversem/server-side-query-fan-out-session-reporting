#!/usr/bin/env python3
"""
CLI script to run data quality checks.

Usage:
    # Run all quality checks
    python scripts/run_quality_checks.py

    # Run for specific date
    python scripts/run_quality_checks.py --date 2024-01-15

    # Run specific table checks
    python scripts/run_quality_checks.py --table bot_requests_daily
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.pipeline import setup_logging
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
        description="Run data quality checks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all quality checks
  python scripts/run_quality_checks.py

  # Run for specific date
  python scripts/run_quality_checks.py --date 2024-01-15

  # Run specific table checks
  python scripts/run_quality_checks.py --table bot_requests_daily

  # Output as JSON
  python scripts/run_quality_checks.py --json
        """,
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )
    parser.add_argument(
        "--date",
        type=parse_date,
        help="Date to check (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--table",
        choices=["bot_requests_raw", "bot_requests_daily", "query_fanout_sessions"],
        help="Specific table to check",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
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

    # Initialize backend
    kwargs = {}
    if args.db_path:
        kwargs["db_path"] = args.db_path
    backend = get_backend("sqlite", **kwargs)
    backend.initialize()

    if not args.json:
        print()
        print("🔍 Data Quality Checks")
        print("=" * 50)
        print(f"  Backend: sqlite")
        if args.db_path:
            print(f"  Database: {args.db_path}")
        if args.date:
            print(f"  Date: {args.date}")
        if args.table:
            print(f"  Table: {args.table}")
        print()

    try:
        checks = {}
        tables = (
            [args.table]
            if args.table
            else ["bot_requests_raw", "bot_requests_daily", "query_fanout_sessions"]
        )

        for table in tables:
            if not backend.table_exists(table):
                checks[table] = {"exists": False, "row_count": 0}
                continue

            # Get row count
            row_count = backend.get_table_row_count(table)

            # Get date range if applicable
            date_range = None
            if table in ("bot_requests_daily", "query_fanout_sessions"):
                date_col = (
                    "request_date" if table == "bot_requests_daily" else "session_date"
                )
                result = backend.query(
                    f"SELECT MIN({date_col}) as min_date, MAX({date_col}) as max_date FROM {table}"
                )
                if result:
                    date_range = {
                        "min": result[0]["min_date"],
                        "max": result[0]["max_date"],
                    }

            checks[table] = {
                "exists": True,
                "row_count": row_count,
                "date_range": date_range,
            }

            # Additional checks for specific date
            if args.date and table in ("bot_requests_daily", "query_fanout_sessions"):
                date_col = (
                    "request_date" if table == "bot_requests_daily" else "session_date"
                )
                result = backend.query(
                    f"SELECT COUNT(*) as count FROM {table} WHERE {date_col} = :date",
                    {"date": args.date.isoformat()},
                )
                checks[table]["date_count"] = result[0]["count"] if result else 0

        if args.json:
            print(json.dumps(checks, indent=2, default=str))
        else:
            all_ok = True
            for table, info in checks.items():
                print(f"\n📋 {table}")
                print("-" * 40)
                if not info["exists"]:
                    print("  ⚠️  Table does not exist")
                    all_ok = False
                else:
                    print(f"  Rows: {info['row_count']:,}")
                    if info.get("date_range"):
                        print(
                            f"  Date range: {info['date_range']['min']} to {info['date_range']['max']}"
                        )
                    if "date_count" in info:
                        print(f"  Records for {args.date}: {info['date_count']:,}")

            print()
            if all_ok:
                print("✅ All quality checks passed")
            else:
                print("⚠️  Some issues found")

        return 0

    finally:
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
