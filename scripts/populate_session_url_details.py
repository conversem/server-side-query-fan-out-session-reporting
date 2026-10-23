#!/usr/bin/env python3
"""
Populate session_url_details table from query_fanout_sessions.

Creates a flattened view of session URLs for dashboarding, where each URL
from the url_list array gets its own row.

Usage:
    # Populate all data (full backfill)
    python scripts/populate_session_url_details.py --project-id PROJECT_ID

    # Populate specific date range
    python scripts/populate_session_url_details.py --project-id PROJECT_ID \
        --start-date 2024-01-01 --end-date 2024-01-31

    # Dry run (show what would be done)
    python scripts/populate_session_url_details.py --project-id PROJECT_ID --dry-run

    # Show summary statistics
    python scripts/populate_session_url_details.py --project-id PROJECT_ID --status
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.reporting.session_aggregations import SessionAggregator
from llm_bot_pipeline.storage import get_backend

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Populate session_url_details table from query_fanout_sessions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full backfill (all data)
  python scripts/populate_session_url_details.py --project-id my-project

  # Specific date range
  python scripts/populate_session_url_details.py --project-id my-project \\
      --start-date 2024-01-01 --end-date 2024-01-31

  # Dry run
  python scripts/populate_session_url_details.py --project-id my-project --dry-run

  # Show current status
  python scripts/populate_session_url_details.py --project-id my-project --status

  # Use SQLite backend instead of BigQuery
  python scripts/populate_session_url_details.py --backend sqlite
        """,
    )

    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview operations without making changes",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current status and exit",
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="Don't delete existing data before inserting",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCP project ID (or set GCP_PROJECT_ID env var)",
    )
    parser.add_argument(
        "--credentials",
        default=os.environ.get("GCP_SERVICE_ACCOUNT_KEY"),
        help="Path to service account JSON key (or set GCP_SERVICE_ACCOUNT_KEY env var)",
    )
    parser.add_argument(
        "--backend",
        default="sqlite",
        choices=["bigquery", "sqlite"],
        help="Storage backend to use (default: sqlite)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.backend == "bigquery" and not args.project_id:
        parser.error("--project-id is required for BigQuery (or set GCP_PROJECT_ID)")

    return args


def show_status(aggregator: SessionAggregator) -> None:
    """Show current status of both tables."""
    print("\nSession URL Details Status")
    print("=" * 60)

    session_summary = aggregator.get_session_summary()
    print("\n  Query Fanout Sessions:")
    print("  " + "-" * 56)
    if "error" in session_summary:
        print(f"    Error: {session_summary['error']}")
    else:
        print(f"    Total sessions: {session_summary.get('total_sessions', 0):,}")
        print(f"    Total requests: {session_summary.get('total_requests', 0):,}")
        print(f"    Date range: {session_summary.get('date_range', {})}")

    url_summary = aggregator.get_url_details_summary()
    print("\n  Session URL Details:")
    print("  " + "-" * 56)
    if "error" in url_summary:
        print(f"    Error: {url_summary['error']}")
    else:
        print(f"    Total rows: {url_summary.get('total_rows', 0):,}")
        print(f"    Unique sessions: {url_summary.get('unique_sessions', 0):,}")
        print(f"    Unique URLs: {url_summary.get('unique_urls', 0):,}")
        print(f"    Unique providers: {url_summary.get('unique_providers', 0):,}")
        print(
            f"    Date range: {url_summary.get('earliest_date')} to "
            f"{url_summary.get('latest_date')}"
        )

    print()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    backend_kwargs = {}
    if args.backend == "bigquery":
        backend_kwargs = {
            "project_id": args.project_id,
            "credentials_path": args.credentials,
        }

    backend = get_backend(args.backend, **backend_kwargs)
    aggregator = SessionAggregator(backend=backend)

    try:
        aggregator.initialize()

        if args.status:
            show_status(aggregator)
            return 0

        if args.dry_run:
            print("\nDry Run - No changes will be made")
            print("=" * 60)
            print(f"  Backend: {args.backend}")
            if args.project_id:
                print(f"  Project: {args.project_id}")
            print(
                f"  Date range: {args.start_date or 'all'} to "
                f"{args.end_date or 'all'}"
            )
            print(f"  Delete existing: {not args.no_delete}")
            print()
            show_status(aggregator)
            return 0

        print("\nPopulating Session URL Details")
        print("=" * 60)
        print(f"  Backend: {args.backend}")
        if args.project_id:
            print(f"  Project: {args.project_id}")
        print(f"  Date range: {args.start_date or 'all'} to {args.end_date or 'all'}")
        print(f"  Delete existing: {not args.no_delete}")
        print()

        rows_inserted = aggregator.populate_url_details(
            start_date=args.start_date,
            end_date=args.end_date,
            delete_existing=not args.no_delete,
        )

        print()
        print("=" * 60)
        print(f"Completed: {rows_inserted:,} rows inserted")
        print()

        show_status(aggregator)

        return 0

    except Exception as e:
        logger.exception(f"Failed: {e}")
        return 1

    finally:
        aggregator.close()
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
