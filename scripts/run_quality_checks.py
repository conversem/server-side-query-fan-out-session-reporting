#!/usr/bin/env python3
"""
CLI script to run data quality checks on pipeline data.

Usage:
    # Run all checks for yesterday
    python scripts/run_quality_checks.py --project-id PROJECT_ID

    # Run checks for specific date
    python scripts/run_quality_checks.py --project-id PROJECT_ID --date 2024-01-15

    # Run checks on a specific table
    python scripts/run_quality_checks.py --project-id PROJECT_ID --table daily_summary

    # Output results as JSON
    python scripts/run_quality_checks.py --project-id PROJECT_ID --json
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.monitoring import (
    DataQualityChecker,
    DataQualityReport,
    QualityStatus,
)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def print_report(report: DataQualityReport, verbose: bool = False) -> None:
    """Print quality report in human-readable format."""
    # Status emoji
    status_emoji = {
        QualityStatus.PASS: "✅",
        QualityStatus.WARN: "⚠️",
        QualityStatus.FAIL: "❌",
        QualityStatus.SKIP: "⏭️",
    }

    print()
    print("=" * 60)
    print(f"📊 Data Quality Report: {report.table_name}")
    print("=" * 60)
    print(f"Check Date: {report.check_date}")
    print(
        f"Overall Status: {status_emoji.get(report.overall_status, '❓')} {report.overall_status.value.upper()}"
    )
    print()

    # Summary
    summary = report.summary
    print(
        f"Summary: {summary['passed']} passed, {summary['warnings']} warnings, "
        f"{summary['failed']} failed, {summary['skipped']} skipped"
    )
    print()

    # Individual checks
    print("Check Results:")
    print("-" * 60)

    for result in report.results:
        emoji = status_emoji.get(result.status, "❓")
        print(f"  {emoji} {result.check_name}: {result.message}")

        if verbose and result.details:
            for key, value in result.details.items():
                if key not in ("table_id", "error"):
                    print(f"      {key}: {value}")

    print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run data quality checks on pipeline data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all checks for yesterday
  python scripts/run_quality_checks.py --project-id my-project

  # Run checks for specific date
  python scripts/run_quality_checks.py --project-id my-project --date 2024-01-15

  # Run checks on daily_summary table
  python scripts/run_quality_checks.py --project-id my-project --table daily_summary

  # Output JSON results
  python scripts/run_quality_checks.py --project-id my-project --json
        """,
    )

    # Required arguments
    parser.add_argument(

    # Optional arguments
    parser.add_argument(
        "--date",
        type=parse_date,
        default=None,
        help="Date to check (YYYY-MM-DD, default: yesterday)",
    )
    parser.add_argument(
        "--table",
        default="bot_requests_daily",
        help="Table name to check (default: bot_requests_daily)",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help="Path to service account JSON key file",
    )
    parser.add_argument(
        "--dataset",
        default="llm_report",
        help="Dataset name (default: llm_report)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed check information",
    )
    parser.add_argument(
        "--skip-variance",
        action="store_true",
        help="Skip variance check (useful for new tables)",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    # Determine check date
    check_date = args.date or (date.today() - timedelta(days=1))

    # Initialize checker
    credentials_path = Path(args.credentials) if args.credentials else None

    try:
        checker = DataQualityChecker(
            project_id=args.project_id,
            credentials_path=credentials_path,
            dataset_report=args.dataset,
        )
    except Exception as e:
        print(f"❌ Failed to initialize checker: {e}", file=sys.stderr)
        return 1

    # Run checks
    if not args.json:
        print(f"\n🔍 Running data quality checks for {check_date}...")

    try:
        report = checker.run_all_checks(
            table_name=args.table,
            check_date=check_date,
            skip_variance=args.skip_variance,
        )
    except Exception as e:
        print(f"❌ Quality check failed: {e}", file=sys.stderr)
        return 1

    # Output results
    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print_report(report, verbose=args.verbose)

    # Exit code based on result
    if report.overall_status == QualityStatus.FAIL:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

