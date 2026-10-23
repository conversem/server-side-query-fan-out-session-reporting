"""Nightly auto-backfill: continue on day failure, stop starting past budget."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from .missing_days import (
    REASON_EMPTY_SOURCE,
    MissingDay,
    effective_lookback_days,
    has_cloudflare_domains,
    list_missing_days,
    nightly_window_start,
    plan_nightly_dates,
)

if TYPE_CHECKING:
    from ...config.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DayRunPolicy:
    """Per-date ingest/sitemap/quality flags for one auto-backfill day."""

    skip_ingestion: bool
    skip_sitemap: bool
    only_domains: list[str] | None
    skip_quality: bool
    track_freshness: bool
    emit_failure_alert: bool


def should_use_auto_backfill(
    processing_mode: str,
    *,
    explicit_date: bool,
    explicit_start_date: bool,
    days: int,
    no_auto_backfill: bool,
    enabled: bool,
) -> bool:
    """Discovery iff gcp_bq, no --date/--start-date, days == 1, and enabled."""
    explicit_range = bool(explicit_date or explicit_start_date or days != 1)
    return (
        processing_mode == "gcp_bq"
        and not explicit_range
        and not no_auto_backfill
        and enabled
    )


def build_day_policy(
    day: date, yesterday: date, missing: list[MissingDay]
) -> DayRunPolicy:
    """Per-date ingest/sitemap/quality policy for the nightly auto path."""
    if day == yesterday:
        return DayRunPolicy(
            skip_sitemap=False,
            skip_ingestion=False,
            only_domains=None,
            skip_quality=True,
            track_freshness=True,
            emit_failure_alert=True,
        )
    day_rows = [item for item in missing if item.day == day]
    empty_domains = [
        item.domain for item in day_rows if item.reason == REASON_EMPTY_SOURCE
    ]
    if empty_domains:
        return DayRunPolicy(
            only_domains=empty_domains,
            skip_ingestion=False,
            skip_sitemap=True,
            skip_quality=True,
            track_freshness=False,
            emit_failure_alert=False,
        )
    return DayRunPolicy(
        skip_ingestion=True,
        skip_sitemap=True,
        only_domains=None,
        skip_quality=True,
        track_freshness=False,
        emit_failure_alert=False,
    )


def _is_retention_error(error: object) -> bool:
    return "retention limit" in str(error).lower()


def run_auto_backfill(
    dates: list[date],
    daily_runner: Callable[..., dict],
    shutdown_requested: Callable[[], bool],
    yesterday: date,
    time_budget_seconds: int,
    elapsed_seconds: Callable[[], float],
    day_policy: Callable[[date], DayRunPolicy],
    dry_run: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
) -> dict:
    aggregated = {
        "success": True,
        "start_date": str(min(dates)) if dates else None,
        "end_date": str(max(dates)) if dates else None,
        "days_processed": 0,
        "days_failed": 0,
        "days_skipped_budget": 0,
        "days_skipped_shutdown": 0,
        "days_skipped_retention": 0,
        "leftover_dates": [],
        "error": None,
        "errors": [],
    }
    yesterday_ok = False

    for index, day in enumerate(dates):
        if elapsed_seconds() >= time_budget_seconds:
            leftover = dates[index:]
            aggregated["days_skipped_budget"] = len(leftover)
            aggregated["leftover_dates"] = [d.isoformat() for d in leftover]
            logger.warning(
                "[AutoBackfill] Time budget reached; leaving %s for tomorrow",
                aggregated["leftover_dates"],
            )
            break

        policy = day_policy(day)
        logger.info("[AutoBackfill] Processing %s", day)
        try:
            day_result = daily_runner(
                start_date=day,
                end_date=day,
                dry_run=dry_run,
                skip_ingestion=policy.skip_ingestion,
                skip_sessions=skip_sessions,
                skip_url_details=skip_url_details,
                skip_sitemap=policy.skip_sitemap,
                only_domains=policy.only_domains,
                skip_quality=policy.skip_quality,
                track_freshness=policy.track_freshness,
                emit_failure_alert=policy.emit_failure_alert,
            )
        except ValueError as exc:
            if not _is_retention_error(exc):
                raise
            day_result = {"success": False, "error": str(exc)}
        error_text = str(day_result.get("error") or "")
        if not day_result.get("success") and _is_retention_error(error_text):
            aggregated["days_skipped_retention"] += 1
            logger.warning("[AutoBackfill] Skipping %s: %s", day, error_text)
            continue
        if day_result.get("success"):
            aggregated["days_processed"] += 1
            if day == yesterday:
                yesterday_ok = True
        else:
            aggregated["days_failed"] += 1
            aggregated["success"] = False
            err = day_result.get("error", "Unknown error")
            aggregated["errors"].append({"date": day.isoformat(), "error": err})
            if not aggregated["error"]:
                aggregated["error"] = err
            logger.error("[AutoBackfill] Day failed %s: %s", day, err)

        if shutdown_requested():
            remaining = dates[index + 1 :]
            aggregated["leftover_dates"] = [d.isoformat() for d in remaining]
            aggregated["days_skipped_shutdown"] = len(remaining)
            logger.warning("[AutoBackfill] Shutdown requested after %s", day)
            break

    if dates and yesterday in dates and not yesterday_ok:
        aggregated["success"] = False
        if not aggregated["error"]:
            aggregated["error"] = f"Yesterday {yesterday.isoformat()} was not processed"
    return aggregated


def run_auto_backfill_pipeline(
    *,
    yesterday: date,
    settings: Settings,
    daily_runner: Callable[..., dict],
    config: dict,
    resolve_creds: Callable[[dict], tuple[str, str]],
    get_backend: Callable[..., Any],
    shutdown_requested: Callable[[], bool],
    run_quality_stage: Callable[..., None],
    dry_run: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
) -> dict:
    """Discover missing days and run the nightly auto-backfill loop."""
    domains = list(settings.domains or [])
    has_cloudflare = has_cloudflare_domains(domains)
    scheduled = [yesterday]
    missing: list[MissingDay] = []
    backend = None

    if not domains:
        logger.warning(
            "[AutoBackfill] No domains configured; skipping discovery (yesterday only)"
        )
    else:
        now_utc = datetime.now(timezone.utc)
        respect = settings.auto_backfill.respect_cloudflare_retention
        lookback = effective_lookback_days(
            settings.auto_backfill.lookback_days,
            has_cloudflare=has_cloudflare,
            respect_cloudflare_retention=respect,
        )
        window_start = nightly_window_start(
            yesterday, now_utc, lookback, has_cloudflare, respect
        )
        try:
            project_id, creds_path = resolve_creds(config)
            backend = get_backend(
                backend_type="bigquery",
                project_id=project_id,
                credentials_path=creds_path,
                dataset_raw=settings.dataset_raw,
                dataset_report=settings.dataset_report,
                location=settings.gcp_location,
            )
            logger.info(
                "[AutoBackfill] Discovery scanning raw=%s clean=%s "
                "window_start=%s end_date=%s",
                backend.get_full_table_id("raw_bot_requests"),
                backend.get_full_table_id("bot_requests_daily"),
                window_start,
                yesterday,
            )
            missing = list_missing_days(
                backend=backend,
                domains=[dc.domain for dc in domains],
                start_date=window_start,
                end_date=yesterday,
                min_raw_rows=settings.auto_backfill.min_raw_rows,
                host_suffixes=[dc.host_suffix for dc in domains],
            )
            scheduled = plan_nightly_dates(yesterday, missing, window_start)
        except Exception:
            logger.exception(
                "[AutoBackfill] Discovery failed; falling back to yesterday only"
            )
            scheduled = [yesterday]
            missing = []
        finally:
            if backend is not None:
                try:
                    backend.close()
                except Exception:
                    logger.warning(
                        "[AutoBackfill] Failed to close discovery backend",
                        exc_info=True,
                    )

    started = time.monotonic()
    result = run_auto_backfill(
        dates=scheduled,
        daily_runner=daily_runner,
        shutdown_requested=shutdown_requested,
        yesterday=yesterday,
        time_budget_seconds=settings.auto_backfill.time_budget_seconds,
        elapsed_seconds=lambda: time.monotonic() - started,
        day_policy=lambda day: build_day_policy(day, yesterday, missing),
        dry_run=dry_run,
        skip_sessions=skip_sessions,
        skip_url_details=skip_url_details,
    )
    if not dry_run:
        run_quality_stage(config, yesterday, logger)
    return result
