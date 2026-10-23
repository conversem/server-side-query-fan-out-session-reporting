"""Backfill orchestration for the daily ETL runner."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Callable

from ..backfill_state import BackfillStateManager


def run_backfill(
    start_date: date,
    end_date: date,
    state_path: Path,
    daily_runner: Callable[..., dict],
    shutdown_requested: Callable[[], bool],
    force_restart: bool = False,
    dry_run: bool = False,
    skip_ingestion: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
    skip_sitemap: bool = False,
) -> dict:
    """Run backfill day-by-day with resume capability."""
    logger = logging.getLogger(__name__)
    state = BackfillStateManager(state_path)

    if force_restart:
        state.clear_range(start_date, end_date)
        logger.info(
            "[Backfill] Force restart: cleared state for %s to %s", start_date, end_date
        )

    dates_to_process = state.get_dates_to_process(start_date, end_date)
    completed_count = (end_date - start_date).days + 1 - len(dates_to_process)

    if completed_count > 0:
        logger.info(
            "[Backfill] Resuming: %d dates already completed, %d remaining",
            completed_count,
            len(dates_to_process),
        )

    if not dates_to_process:
        logger.info("[Backfill] All dates already completed")
        return {
            "success": True,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "stages": {},
            "resumed": True,
            "days_skipped": completed_count,
        }

    aggregated = {
        "success": True,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "stages": {},
        "days_processed": 0,
        "days_failed": 0,
        "error": None,
    }

    for i, day in enumerate(dates_to_process):
        logger.info(
            "[Backfill] Processing day %d/%d: %s", i + 1, len(dates_to_process), day
        )
        day_result = daily_runner(
            start_date=day,
            end_date=day,
            dry_run=dry_run,
            skip_ingestion=skip_ingestion,
            skip_sessions=skip_sessions,
            skip_url_details=skip_url_details,
            skip_sitemap=skip_sitemap,
        )
        if day_result.get("success"):
            aggregated["days_processed"] += 1
            if not dry_run:
                state.record_completed(day, start_date, end_date)
        else:
            aggregated["days_failed"] += 1
            aggregated["success"] = False
            aggregated["error"] = day_result.get("error", "Unknown error")
            logger.error("[Backfill] Stopping after failure on %s", day)
            break

        if shutdown_requested():
            logger.warning("[Backfill] Shutdown requested - stopping after %s", day)
            break

    return aggregated
