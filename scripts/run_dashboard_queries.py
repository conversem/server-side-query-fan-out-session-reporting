#!/usr/bin/env python3
"""
CLI script to run dashboard queries for KPIs and metrics.

Usage:
    # Run all KPIs
    python scripts/run_dashboard_queries.py --all

    # Run specific KPIs
    python scripts/run_dashboard_queries.py --kpi requests_per_day --kpi top_bots
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
from llm_bot_pipeline.reporting import LocalDashboardQueries
from llm_bot_pipeline.storage import get_backend


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


AVAILABLE_KPIS = [
    "requests_per_day",
    "top_bots",
    "bot_category_breakdown",
    "requests_by_provider",
    "top_url_paths",
    "response_status_breakdown",
    "session_summary",
]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run dashboard queries for KPIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all KPIs
  python scripts/run_dashboard_queries.py --all

  # Run specific KPIs
  python scripts/run_dashboard_queries.py --kpi requests_per_day --kpi top_bots

  # Output as JSON
  python scripts/run_dashboard_queries.py --all --json
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
        "--all",
        action="store_true",
        help="Run all KPI queries",
    )
    parser.add_argument(
        "--kpi",
        action="append",
        choices=AVAILABLE_KPIS,
        help=f"KPI to run (can specify multiple). Available: {', '.join(AVAILABLE_KPIS)}",
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

    if not args.all and not args.kpi:
        parser.error("Must specify --all or at least one --kpi")

    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize backend
    kwargs = {}
    if args.db_path:
        kwargs["db_path"] = args.db_path
    backend = get_backend("sqlite", **kwargs)
    backend.initialize()

    kpis_to_run = AVAILABLE_KPIS if args.all else args.kpi

    if not args.json:
        print()
        print("📊 Dashboard Queries")
        print("=" * 50)
        print(f"  Backend: sqlite")
        if args.db_path:
            print(f"  Database: {args.db_path}")
        print(f"  KPIs: {', '.join(kpis_to_run)}")
        print()

    try:
        queries = LocalDashboardQueries(backend=backend)
        results = {}

        for kpi in kpis_to_run:
            method = getattr(queries, f"get_{kpi}", None)
            if method:
                result = method(
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
                results[kpi] = result

        if args.json:
            # Convert results for JSON output
            json_results = {}
            for kpi, result in results.items():
                if hasattr(result, "data"):
                    json_results[kpi] = result.data
                else:
                    json_results[kpi] = result
            print(json.dumps(json_results, indent=2, default=str))
        else:
            for kpi, result in results.items():
                print(f"\n📈 {kpi}")
                print("-" * 40)
                if hasattr(result, "data"):
                    for row in result.data[:10]:  # Show top 10
                        print(f"  {row}")
                else:
                    print(f"  {result}")

        return 0

    finally:
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
