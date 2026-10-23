"""CLI argument parsing for the daily ETL runner."""

from __future__ import annotations

import argparse
from pathlib import Path


def parse_daily_args(default_log_dir: Path) -> argparse.Namespace:
    """Parse command-line arguments for ``scripts/run_daily_etl.py``."""
    parser = argparse.ArgumentParser(
        description="Daily ETL Pipeline: Cloudflare Logpull -> BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Nightly: yesterday plus missing days in the retention-safe lookback (gcp_bq)
  python scripts/run_daily_etl.py

  # Run for specific date (no discovery)
  python scripts/run_daily_etl.py --date 2026-01-25

  # Backfill date range (resumes from last completed date on restart; no discovery)
  python scripts/run_daily_etl.py --start-date 2026-01-20 --end-date 2026-01-25

  # Backfill from scratch (ignore saved progress)
  python scripts/run_daily_etl.py --start-date 2026-01-20 --end-date 2026-01-25 --force-restart

  # Preview without making changes
  python scripts/run_daily_etl.py --dry-run

  # Skip-only: disable discovery, then skip ingest
  python scripts/run_daily_etl.py --no-auto-backfill --skip-ingestion

Scheduling:
  Nightly Cloud Run and cron_daily_etl.sh: unflagged run_daily_etl.py (gcp_bq).
  Yesterday plus missing days in the retention-safe lookback.
  --date / --start-date / --days != 1 / --no-auto-backfill disable discovery.
  daily_pipeline.sh is the old multi-domain explicit-range path (no discovery).
  See docs/sops/scheduling.md.
        """,
    )

    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--date", type=str, help="Process specific date (YYYY-MM-DD)"
    )
    date_group.add_argument(
        "--start-date",
        type=str,
        help="Start date for range (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for range (YYYY-MM-DD), requires --start-date",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help=(
            "Number of days to process (default: 1 = yesterday). "
            "On gcp_bq, 1 also discovers missing days; any other value disables discovery."
        ),
    )

    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help=(
            "Skip Cloudflare ingestion (use existing raw data). "
            "On the auto path, yesterday is still ingested; "
            "use --no-auto-backfill for skip-only."
        ),
    )
    parser.add_argument(
        "--skip-sessions",
        action="store_true",
        help="Skip session creation stage",
    )
    parser.add_argument(
        "--skip-url-details",
        action="store_true",
        help="Skip URL details population",
    )
    parser.add_argument(
        "--skip-sitemap",
        action="store_true",
        help=(
            "Skip sitemap fetch and aggregation. "
            "On the auto path, yesterday still fetches sitemaps; "
            "use --no-auto-backfill for skip-only."
        ),
    )
    parser.add_argument(
        "--no-auto-backfill",
        action="store_true",
        help="Process yesterday only; skip missing-day discovery",
    )

    parser.add_argument(
        "--processing-mode",
        type=str,
        choices=["local_sqlite", "local_bq_buffered", "local_bq_streaming", "gcp_bq"],
        default=None,
        help="Override processing mode from config",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview operations without making changes",
    )
    parser.add_argument(
        "--force-restart",
        action="store_true",
        help="Ignore backfill state and start from scratch (gcp_bq backfill only)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Use structured JSON logging (cloud environments)",
    )

    parser.add_argument(
        "--log-dir",
        type=str,
        default=str(default_log_dir),
        help=f"Directory for log files (default: {default_log_dir})",
    )
    parser.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable file logging (console only)",
    )

    return parser.parse_args()
