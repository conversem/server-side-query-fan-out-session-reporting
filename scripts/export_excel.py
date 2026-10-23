#!/usr/bin/env python3
"""
Export data from SQLite or BigQuery to Excel (.xlsx).

Usage:
    # Full report from SQLite (default)
    python scripts/export_excel.py --output report.xlsx

    # Specific tables
    python scripts/export_excel.py --tables daily_summary,url_performance --output report.xlsx

    # Date-filtered
    python scripts/export_excel.py --start-date 2026-01-01 --end-date 2026-01-31

    # From BigQuery
    python scripts/export_excel.py --backend bigquery --output report.xlsx

    # Custom SQL query
    python scripts/export_excel.py --query "SELECT * FROM daily_summary LIMIT 100" --output query.xlsx
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from llm_bot_pipeline.config.constants import VALID_TABLE_NAMES
from llm_bot_pipeline.config.logging_config import setup_logging
from llm_bot_pipeline.config.settings import get_settings
from llm_bot_pipeline.reporting.excel_exporter import ExcelExporter
from llm_bot_pipeline.storage import get_backend
from llm_bot_pipeline.utils.date_utils import parse_date
from llm_bot_pipeline.utils.path_utils import validate_path_safe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export pipeline data to Excel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--backend",
        choices=["sqlite", "bigquery"],
        default=None,
        help="Storage backend (default: from config)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="report.xlsx",
        help="Output file path (default: report.xlsx)",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Comma-separated table names, or --all for default set",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all default report tables",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Custom SQL query to export",
    )
    parser.add_argument(
        "--allow-raw-sql",
        action="store_true",
        default=False,
        help="Allow dangerous raw SQL queries via --query (security risk!)",
    )
    parser.add_argument(
        "--sheet-name",
        type=str,
        default="Results",
        help="Sheet name for --query mode (default: Results)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Row limit per table",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Use structured JSON logging (cloud environments)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validate_path_safe(args.output, PROJECT_ROOT)
    setup_logging(
        level=logging.DEBUG if args.verbose else logging.INFO,
        json_logs=args.json_logs,
    )
    logger = logging.getLogger(__name__)

    settings = get_settings()

    backend_type = args.backend or settings.storage_backend
    backend = get_backend(backend_type, **settings.backend_kwargs())

    try:
        backend.initialize()
        exporter = ExcelExporter(backend)

        start = parse_date(args.start_date) if args.start_date else None
        end = parse_date(args.end_date) if args.end_date else None

        if args.query:
            if not args.allow_raw_sql:
                logger.error("Raw SQL queries require the --allow-raw-sql flag.")
                return 1
            logger.warning("Executing raw SQL query - high risk!")
            path = exporter.export_query(
                sql=args.query,
                output_path=args.output,
                sheet_name=args.sheet_name,
            )
        elif args.tables:
            tables = [t.strip() for t in args.tables.split(",")]
            for table in tables:
                if table not in VALID_TABLE_NAMES:
                    logger.error(
                        "Invalid table: %s. Valid: %s", table, sorted(VALID_TABLE_NAMES)
                    )
                    return 1
            path = exporter.export_report(
                output_path=args.output,
                tables=tables,
                start_date=start,
                end_date=end,
            )
        else:
            path = exporter.export_report(
                output_path=args.output,
                start_date=start,
                end_date=end,
            )

        logger.info("Export complete: %s", path)
        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130

    except Exception as e:
        logger.exception("Export failed: %s", e)
        return 1

    finally:
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
