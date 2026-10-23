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
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config.settings import get_settings
from llm_bot_pipeline.monitoring.quality_runner import (
    create_quality_backend,
    run_quality_checks,
)
from llm_bot_pipeline.pipeline import setup_logging
from llm_bot_pipeline.storage import get_backend
from llm_bot_pipeline.utils.date_utils import parse_date


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
        "--backend",
        choices=["sqlite", "bigquery"],
        default=None,
        help="Storage backend (default: from settings; cloud deployments use bigquery)",
    )
    parser.add_argument(
        "--project-id",
        help="GCP project ID for BigQuery quality checks (default: settings)",
    )
    parser.add_argument(
        "--credentials-path",
        type=Path,
        help="Service-account JSON path for BigQuery checks (default: ADC/settings)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: from settings)",
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
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Use structured JSON logging (cloud environments)",
    )
    parser.add_argument(
        "--comprehensive",
        action="store_true",
        help="Deprecated: unified quality checks are now the default.",
    )
    parser.add_argument(
        "--inventory",
        action="store_true",
        help="Run the legacy table inventory instead of unified quality checks.",
    )
    parser.add_argument(
        "--skip-variance",
        action="store_true",
        help="Skip day-over-day count variance check.",
    )

    args = parser.parse_args()

    setup_logging(
        level=logging.DEBUG if args.verbose else logging.INFO,
        json_logs=args.json_logs,
    )
    logger = logging.getLogger(__name__)

    settings = get_settings()
    backend_type = args.backend or settings.storage_backend
    project_id = args.project_id or settings.gcp_project_id or None
    credentials_path = args.credentials_path or settings.service_account_key_path

    # Initialize backend
    if args.inventory:
        kwargs = {}
        if args.db_path:
            kwargs["db_path"] = args.db_path
        if backend_type == "bigquery":
            if project_id:
                kwargs["project_id"] = project_id
            if credentials_path:
                kwargs["credentials_path"] = credentials_path
        backend = get_backend(backend_type, **kwargs)
    else:
        backend = create_quality_backend(
            backend_type=backend_type,
            project_id=project_id,
            credentials_path=credentials_path,
            db_path=args.db_path,
        )
    backend.initialize()

    if not args.json:
        print()
        print("🔍 Data Quality Checks")
        print("=" * 50)
        print(f"  Backend: {backend.backend_type}")
        if backend.backend_type == "bigquery" and project_id:
            print(f"  GCP project: {project_id}")
        if args.db_path:
            print(f"  Database: {args.db_path}")
        if args.date:
            print(f"  Date: {args.date}")
        if args.table:
            print(f"  Table: {args.table}")
        print()

    try:
        if not args.inventory:
            report = run_quality_checks(
                backend=backend,
                domain_configs=settings.domains,
                table_name=args.table or "bot_requests_daily",
                check_date=args.date,
                skip_variance=args.skip_variance,
            )
            if args.json:
                print(json.dumps(report.to_dict(), indent=2, default=str))
            else:
                print(f"Status: {report.overall_status.value}")
                print(
                    "Summary: "
                    f"{report.summary['passed']} passed, "
                    f"{report.summary['warnings']} warning(s), "
                    f"{report.summary['failed']} failed"
                )
                for result in report.results:
                    print(
                        f"- {result.status.value}: {result.check_name}: {result.message}"
                    )
            return 0 if report.passed else 1

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
