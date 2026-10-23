#!/usr/bin/env python3
"""
Daily ETL Pipeline Script.

Complete end-to-end pipeline that:
1. Pulls logs from Cloudflare Logpull API
2. Transforms raw data to clean tables
3. Creates aggregations and sessions
4. Populates URL details for dashboarding

Unflagged gcp_bq nightly (Cloud Run / cron_daily_etl.sh) processes yesterday,
then missing days in the retention-safe lookback. Completeness still only logs
holes. --date / --start-date / --days != 1 / --no-auto-backfill disable discovery.

On the auto path, --skip-ingestion and --skip-sitemap are ignored for yesterday.
Use --no-auto-backfill with those flags for skip-only.

Designed to run daily at 4AM via systemd timer or cron.

Usage:
    # Nightly: yesterday, then retention-safe holes (gcp_bq)
    python scripts/run_daily_etl.py

    # Run for specific date (no discovery)
    python scripts/run_daily_etl.py --date 2026-01-25

    # Run for date range (backfill, no discovery)
    python scripts/run_daily_etl.py --start-date 2026-01-20 --end-date 2026-01-25

    # Dry run (preview only)
    python scripts/run_daily_etl.py --dry-run

    # Skip-only: disable discovery, then skip ingest / sitemap
    python scripts/run_daily_etl.py --no-auto-backfill --skip-ingestion
    python scripts/run_daily_etl.py --no-auto-backfill --skip-sitemap

    # Skip specific stages (non-auto / after --no-auto-backfill)
    python scripts/run_daily_etl.py --skip-ingestion  # Use existing raw data
    python scripts/run_daily_etl.py --skip-sessions   # Skip session creation

Environment:
    SOPS_AGE_KEY_FILE: Path to age key file (default: ~/.sops/age/keys.txt)
    GCP_PROJECT_ID: Override project ID from config
    LOG_DIR: Directory for log files (default: logs/)
"""

import logging
import signal
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# Add src to path for imports
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from llm_bot_pipeline.config.logging_config import setup_file_logging, setup_logging
from llm_bot_pipeline.pipeline.daily.auto_backfill import (
    run_auto_backfill_pipeline as _run_auto_backfill_pipeline,
)
from llm_bot_pipeline.pipeline.daily.auto_backfill import (
    should_use_auto_backfill,
)
from llm_bot_pipeline.pipeline.daily.cli import parse_daily_args
from llm_bot_pipeline.pipeline.daily.config_loader import load_daily_config
from llm_bot_pipeline.pipeline.daily.orchestrator import (
    is_shutdown_requested,
    request_shutdown,
)
from llm_bot_pipeline.pipeline.daily.orchestrator import (
    run_backfill_pipeline as _run_backfill_pipeline,
)
from llm_bot_pipeline.pipeline.daily.orchestrator import (
    run_daily_pipeline as _run_daily_pipeline,
)
from llm_bot_pipeline.pipeline.daily.orchestrator import run_etl_stage as _run_etl_stage
from llm_bot_pipeline.pipeline.daily.orchestrator import (
    run_ingestion_stage as _run_ingestion_stage,
)
from llm_bot_pipeline.pipeline.daily.orchestrator import (
    run_quality_stage as _run_quality_stage,
)
from llm_bot_pipeline.pipeline.daily.orchestrator import (
    run_session_stage as _run_session_stage,
)
from llm_bot_pipeline.pipeline.daily.runtime import (
    build_alert_manager as _build_alert_manager,
)
from llm_bot_pipeline.pipeline.daily.runtime import execute_stage as _execute_stage
from llm_bot_pipeline.pipeline.daily.runtime import (
    log_pipeline_header as _log_pipeline_header,
)
from llm_bot_pipeline.pipeline.daily.runtime import (
    log_pipeline_summary as _log_pipeline_summary,
)
from llm_bot_pipeline.pipeline.daily.runtime import (
    run_post_quality_checks as _run_post_quality_checks,
)
from llm_bot_pipeline.pipeline.daily.stages import ingest_domain as _ingest_domain
from llm_bot_pipeline.pipeline.daily.stages import resolve_creds as _resolve_creds
from llm_bot_pipeline.pipeline.daily.stages import (
    run_aggregations,
    run_etl_pipeline,
    run_ingestion,
    run_sessions,
)
from llm_bot_pipeline.pipeline.daily.stages import (
    run_sitemap_stage as _run_sitemap_stage_bq,
)
from llm_bot_pipeline.pipeline.daily.stages import (
    run_url_details,
)
from llm_bot_pipeline.pipeline.router import run_pipeline as router_run_pipeline
from llm_bot_pipeline.storage import get_backend as _get_backend
from llm_bot_pipeline.utils.date_utils import parse_date, utc_now, utc_yesterday

# -----------------------------------------------------------------------------
# Graceful shutdown (Cloud Run sends SIGTERM)
# -----------------------------------------------------------------------------


def _handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful Cloud Run shutdown."""
    request_shutdown()
    logging.getLogger(__name__).warning(
        "SIGTERM received — finishing current stage then exiting"
    )


signal.signal(signal.SIGTERM, _handle_sigterm)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

CONFIG_PATH = PROJECT_ROOT / "config.enc.yaml"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_SOPS_KEY = Path.home() / ".sops" / "age" / "keys.txt"


def load_pipeline_config() -> dict:
    """Load and return pipeline configuration.

    Resolution order:
        1. GCP Secret Manager (when USE_SECRET_MANAGER=true)
        2. SOPS-encrypted YAML file (local development)
        3. Environment-variable fallback via load_config
    """
    return load_daily_config(CONFIG_PATH, DEFAULT_SOPS_KEY)


def run_daily_pipeline(
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    skip_ingestion: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
    skip_sitemap: bool = False,
    only_domains: Optional[list] = None,
    skip_quality: bool = False,
    track_freshness: bool = True,
    emit_failure_alert: bool = True,
) -> dict:
    """Run the complete daily ETL pipeline."""
    return _run_daily_pipeline(
        config_path=CONFIG_PATH,
        default_sops_key=DEFAULT_SOPS_KEY,
        start_date=start_date,
        end_date=end_date,
        dry_run=dry_run,
        skip_ingestion=skip_ingestion,
        skip_sessions=skip_sessions,
        skip_url_details=skip_url_details,
        skip_sitemap=skip_sitemap,
        only_domains=only_domains,
        skip_quality=skip_quality,
        track_freshness=track_freshness,
        emit_failure_alert=emit_failure_alert,
    )


def run_auto_backfill_pipeline(
    yesterday: date,
    dry_run: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
) -> dict:
    """Discover missing days and run the nightly auto-backfill path."""
    # Inline so tests can patch llm_bot_pipeline.config.settings.get_settings
    # (a module-level binding would break that target).
    from llm_bot_pipeline.config.settings import get_settings

    return _run_auto_backfill_pipeline(
        yesterday=yesterday,
        settings=get_settings(),
        config=load_pipeline_config(),
        daily_runner=run_daily_pipeline,
        resolve_creds=_resolve_creds,
        get_backend=_get_backend,
        shutdown_requested=is_shutdown_requested,
        run_quality_stage=_run_quality_stage,
        dry_run=dry_run,
        skip_sessions=skip_sessions,
        skip_url_details=skip_url_details,
    )


def run_backfill_pipeline(
    start_date: date,
    end_date: date,
    state_path: Path,
    force_restart: bool = False,
    dry_run: bool = False,
    skip_ingestion: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
    skip_sitemap: bool = False,
) -> dict:
    """Run backfill day-by-day with resume capability."""
    return _run_backfill_pipeline(
        config_path=CONFIG_PATH,
        default_sops_key=DEFAULT_SOPS_KEY,
        start_date=start_date,
        end_date=end_date,
        state_path=state_path,
        force_restart=force_restart,
        dry_run=dry_run,
        skip_ingestion=skip_ingestion,
        skip_sessions=skip_sessions,
        skip_url_details=skip_url_details,
        skip_sitemap=skip_sitemap,
    )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def main() -> int:
    """Main entry point."""
    args = parse_daily_args(DEFAULT_LOG_DIR)

    # Setup logging
    log_file = None
    if not args.no_log_file:
        log_file = setup_file_logging(
            Path(args.log_dir), args.verbose, json_logs=args.json_logs
        )
    else:
        setup_logging(
            level=logging.DEBUG if args.verbose else logging.INFO,
            json_logs=args.json_logs,
        )

    logger = logging.getLogger(__name__)

    if log_file:
        logger.info(f"Logging to: {log_file}")

    # Determine date range
    try:
        if args.date:
            start_date = end_date = parse_date(args.date)
        elif args.start_date:
            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date) if args.end_date else start_date
        else:
            # Default: UTC yesterday (or --days ago)
            end_date = utc_yesterday()
            start_date = end_date - timedelta(days=args.days - 1)
    except ValueError as e:
        logger.error(str(e))
        return 1

    # Validate date range
    if end_date < start_date:
        logger.error(f"End date {end_date} is before start date {start_date}")
        return 1

    if end_date >= utc_now().date():
        logger.warning(
            f"End date {end_date} is today or in the future. Data may be incomplete."
        )

    # Inline so tests can patch llm_bot_pipeline.config.settings.get_settings
    # (a module-level binding would break that target).
    from llm_bot_pipeline.config.settings import get_settings

    settings = get_settings()
    processing_mode = args.processing_mode or settings.processing_mode

    if processing_mode == "local_bq_streaming":
        logger.error(
            "local_bq_streaming requires an IngestionRecord iterator; "
            "not supported for daily ETL. Use run_pipeline.py with streaming source."
        )
        return 1

    # Run pipeline
    try:
        if processing_mode == "gcp_bq":
            use_auto = should_use_auto_backfill(
                processing_mode,
                explicit_date=bool(args.date),
                explicit_start_date=bool(args.start_date),
                days=args.days,
                no_auto_backfill=args.no_auto_backfill,
                enabled=settings.auto_backfill.enabled,
            )
            if use_auto:
                yesterday = utc_yesterday()
                results = run_auto_backfill_pipeline(
                    yesterday=yesterday,
                    dry_run=args.dry_run,
                    skip_sessions=args.skip_sessions,
                    skip_url_details=args.skip_url_details,
                )
                return 0 if results.get("success") else 1
            is_backfill = start_date != end_date
            if is_backfill:
                state_path = Path(args.log_dir) / "backfill_state.json"
                results = run_backfill_pipeline(
                    start_date=start_date,
                    end_date=end_date,
                    state_path=state_path,
                    force_restart=args.force_restart,
                    dry_run=args.dry_run,
                    skip_ingestion=args.skip_ingestion,
                    skip_sessions=args.skip_sessions,
                    skip_url_details=args.skip_url_details,
                    skip_sitemap=args.skip_sitemap,
                )
            else:
                results = run_daily_pipeline(
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=args.dry_run,
                    skip_ingestion=args.skip_ingestion,
                    skip_sessions=args.skip_sessions,
                    skip_url_details=args.skip_url_details,
                    skip_sitemap=args.skip_sitemap,
                )
            return 0 if results.get("success") else 1
        else:
            # Dispatch through router for local_sqlite, local_bq_buffered
            logger.info("Processing mode: %s — dispatching via router", processing_mode)
            result = router_run_pipeline(
                start_date=start_date,
                end_date=end_date,
                processing_mode=processing_mode,
                dry_run=args.dry_run,
            )
            success = getattr(result, "success", False)
            return 0 if success else 1

    except KeyboardInterrupt:
        logger.info("\n⏹️  Pipeline interrupted by user")
        return 130

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
