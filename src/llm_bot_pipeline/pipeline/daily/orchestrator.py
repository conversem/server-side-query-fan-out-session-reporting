"""Daily ETL orchestration for cloud BigQuery runs."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from ...monitoring.alerting import create_pipeline_failure_alert
from ...monitoring.pipeline_metrics import track_execution
from .backfill import run_backfill
from .config_loader import load_daily_config
from .runtime import (
    build_alert_manager,
    execute_stage,
    log_pipeline_header,
    log_pipeline_summary,
    run_post_quality_checks,
)
from .stages import (
    run_aggregations,
    run_etl_pipeline,
    run_ingestion,
    run_sessions,
    run_sitemap_stage,
    run_url_details,
)

_shutdown_requested = False


def request_shutdown() -> None:
    """Ask the orchestrator to stop after the current stage."""
    global _shutdown_requested
    _shutdown_requested = True


def is_shutdown_requested() -> bool:
    """Return whether graceful shutdown was requested."""
    return _shutdown_requested


def load_pipeline_config(config_path: Path, default_sops_key: Path) -> dict:
    """Load and return pipeline configuration."""
    return load_daily_config(config_path, default_sops_key)


def run_ingestion_stage(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool,
    logger: logging.Logger,
    only_domains: list[str] | None = None,
) -> dict:
    """Run ingestion stage: provider log source to raw table."""
    return run_ingestion(
        config=config,
        start_date=start_date,
        end_date=end_date,
        dry_run=dry_run,
        logger=logger,
        only_domains=only_domains,
    )


def run_etl_stage(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool,
    logger: logging.Logger,
    track_freshness: bool = True,
) -> dict:
    """Run ETL stage: raw to clean, then reporting aggregations."""
    etl_result = run_etl_pipeline(config, start_date, end_date, dry_run, logger)
    if not etl_result.get("success"):
        return etl_result
    return run_aggregations(
        config,
        start_date,
        end_date,
        dry_run,
        logger,
        track_freshness=track_freshness,
    )


def run_session_stage(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool,
    skip_sessions: bool,
    skip_url_details: bool,
    skip_sitemap: bool,
    logger: logging.Logger,
) -> dict:
    """Run session, URL-details, and sitemap stages."""
    last_result = {"success": True}
    for cond, msg in [
        (skip_sessions, "[Sessions] Skipped (--skip-sessions)"),
        (skip_url_details or skip_sessions, "[URL Details] Skipped"),
        (skip_sitemap, "[Sitemap] Skipped (--skip-sitemap)"),
    ]:
        if cond:
            logger.info(msg)

    if not skip_sessions:
        sess_result = run_sessions(config, start_date, end_date, dry_run, logger)
        if not sess_result.get("success"):
            logger.warning(f"[Sessions] Warning: {sess_result.get('error')}")
        last_result = sess_result

    if not skip_url_details and not skip_sessions:
        url_result = run_url_details(config, start_date, end_date, dry_run, logger)
        if not url_result.get("success"):
            logger.warning(f"[URL Details] Warning: {url_result.get('error')}")
        last_result = url_result

    if not skip_sitemap:
        sitemap_result = run_sitemap_stage(config, dry_run, logger)
        if not sitemap_result.get("success"):
            logger.warning(f"[Sitemap] Warning: {sitemap_result.get('error')}")
        last_result = sitemap_result

    return last_result


def run_quality_stage(config: dict, check_date: date, logger: logging.Logger) -> None:
    """Run post-pipeline data quality checks."""
    run_post_quality_checks(config, check_date, logger)


def run_daily_pipeline(
    config_path: Path,
    default_sops_key: Path,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    skip_ingestion: bool = False,
    skip_sessions: bool = False,
    skip_url_details: bool = False,
    skip_sitemap: bool = False,
    only_domains: list[str] | None = None,
    skip_quality: bool = False,
    track_freshness: bool = True,
    emit_failure_alert: bool = True,
) -> dict:
    """Run the complete daily ETL pipeline."""
    logger = logging.getLogger(__name__)
    started_at = datetime.now(timezone.utc)
    log_pipeline_header(
        logger,
        start_date,
        end_date,
        dry_run,
        skip_ingestion,
        skip_sessions,
        skip_url_details,
        skip_sitemap,
        only_domains=only_domains,
    )
    results = {
        "success": True,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "started_at": started_at.isoformat(),
        "stages": {},
    }
    config: dict = {}
    alert_manager = None

    try:
        with track_execution("daily_etl") as tracker:
            tracker.set_parameters(
                start_date=str(start_date),
                end_date=str(end_date),
                dry_run=dry_run,
            )
            logger.info("[Config] Loading SOPS-encrypted configuration...")
            config = load_pipeline_config(config_path, default_sops_key)
            logger.info(f"[Config] Project: {config.get('gcp', {}).get('project_id')}")
            alert_manager = build_alert_manager(config)

            if not skip_ingestion:
                result = execute_stage(
                    tracker,
                    "ingestion",
                    results,
                    lambda: run_ingestion_stage(
                        config,
                        start_date,
                        end_date,
                        dry_run,
                        logger,
                        only_domains=only_domains,
                    ),
                    record_ingestion=True,
                )
                if not result.get("success"):
                    results["success"] = False
                    raise RuntimeError("Ingestion stage failed")
            else:
                logger.info("[Ingestion] Skipped (--skip-ingestion)")

            if _shutdown_requested:
                logger.warning("Shutdown requested - stopping after current stage")
                raise RuntimeError("Shutdown requested via SIGTERM")

            result = execute_stage(
                tracker,
                "etl",
                results,
                lambda: run_etl_stage(
                    config,
                    start_date,
                    end_date,
                    dry_run,
                    logger,
                    track_freshness=track_freshness,
                ),
            )
            if not result.get("success"):
                results["success"] = False
                raise RuntimeError(f"ETL stage failed: {result.get('error')}")

            if _shutdown_requested:
                logger.warning("Shutdown requested - stopping after current stage")
                raise RuntimeError("Shutdown requested via SIGTERM")

            execute_stage(
                tracker,
                "session",
                results,
                lambda: run_session_stage(
                    config,
                    start_date,
                    end_date,
                    dry_run,
                    skip_sessions,
                    skip_url_details,
                    skip_sitemap,
                    logger,
                ),
            )

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        results["success"] = False
        results["error"] = str(e)

        if alert_manager is not None and emit_failure_alert:
            alert = create_pipeline_failure_alert(
                pipeline_name="daily_etl",
                error=str(e),
                details={
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                },
            )
            alert_manager.send_alert(alert)

    completed_at = datetime.now(timezone.utc)
    results["completed_at"] = completed_at.isoformat()
    results["duration_seconds"] = (completed_at - started_at).total_seconds()
    log_pipeline_summary(logger, results)
    if results["success"] and not dry_run and config and not skip_quality:
        run_quality_stage(config, end_date, logger)
    return results


def run_backfill_pipeline(
    config_path: Path,
    default_sops_key: Path,
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
    return run_backfill(
        start_date=start_date,
        end_date=end_date,
        state_path=state_path,
        daily_runner=lambda **kwargs: run_daily_pipeline(
            config_path=config_path,
            default_sops_key=default_sops_key,
            **kwargs,
        ),
        shutdown_requested=is_shutdown_requested,
        force_restart=force_restart,
        dry_run=dry_run,
        skip_ingestion=skip_ingestion,
        skip_sessions=skip_sessions,
        skip_url_details=skip_url_details,
        skip_sitemap=skip_sitemap,
    )
