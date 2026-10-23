"""Runtime helpers for the daily ETL pipeline."""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Callable

from ...sitemap import run_sitemap_pipeline
from ...storage import get_backend


def run_sitemap_stage_bq(
    config: dict,
    resolve_creds: Callable[[dict], tuple[str, str]],
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict:
    """Run sitemap fetch and aggregation using the BigQuery backend."""
    logger = logger or logging.getLogger(__name__)
    logger.info("[Sitemap] Fetching sitemaps and running aggregations")

    if dry_run:
        logger.info("[Sitemap] DRY RUN - skipping")
        return {"success": True, "urls_stored": 0, "dry_run": True}

    project_id, creds_path = resolve_creds(config)
    backend = get_backend(
        backend_type="bigquery",
        project_id=project_id,
        credentials_path=creds_path,
    )
    try:
        result = run_sitemap_pipeline(backend=backend)
        if result.get("skipped"):
            logger.info("[Sitemap] No sitemap URLs configured - skipped")
        else:
            logger.info(f"[Sitemap] Complete: {result['urls_stored']:,} URLs stored")
        return result
    except Exception as e:
        logger.error(f"[Sitemap] Failed: {e}")
        return {"success": False, "error": str(e)}
    finally:
        backend.close()


def build_alert_manager(config: dict):
    """Create an AlertManager with notifiers based on pipeline config."""
    from ...monitoring.alerting import AlertManager, LogNotifier, WebhookNotifier

    manager = AlertManager()
    manager.add_notifier(LogNotifier())

    monitoring_cfg = config.get("monitoring", {})
    for channel in monitoring_cfg.get("alert_channels", []):
        ch_type = channel.get("type", "")
        url = channel.get("url", "")
        if ch_type in ("webhook", "slack", "discord") and url:
            try:
                manager.add_notifier(WebhookNotifier(url, webhook_type=ch_type))
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"Failed to setup {ch_type} notifier: {e}"
                )
    return manager


def run_post_quality_checks(config: dict, check_date: date, logger) -> None:
    """Run DataQualityChecker post-pipeline without failing the ETL run."""
    try:
        from ...config.settings import DomainConfig
        from ...monitoring.quality_runner import run_quality_checks_with_backend

        project_id = config.get("gcp", {}).get("project_id")
        creds_path = config.get("gcp", {}).get("service_account_key_path")
        backend_type = config.get("storage", {}).get("backend", "bigquery")

        domain_configs = [DomainConfig.from_dict(d) for d in config.get("domains", [])]
        report = run_quality_checks_with_backend(
            backend_type=backend_type,
            domain_configs=domain_configs,
            check_date=check_date,
            project_id=project_id,
            credentials_path=creds_path,
        )
        logger.info(
            "[Quality] Post-pipeline checks: %s (passed=%d warn=%d fail=%d)",
            report.overall_status.value,
            report.summary["passed"],
            report.summary["warnings"],
            report.summary["failed"],
        )
    except Exception as e:
        logger.warning("[Quality] Post-pipeline checks skipped: %s", e)


def log_pipeline_header(
    logger,
    start_date,
    end_date,
    dry_run,
    skip_ingestion,
    skip_sessions,
    skip_url_details,
    skip_sitemap,
    only_domains=None,
) -> None:
    """Log pipeline startup header."""
    logger.info("=" * 60)
    logger.info("Daily ETL Pipeline")
    logger.info("=" * 60)
    logger.info(f"  Date range: {start_date} to {end_date}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info(f"  Skip ingestion: {skip_ingestion}")
    logger.info(f"  Skip sessions: {skip_sessions}")
    logger.info(f"  Skip URL details: {skip_url_details}")
    logger.info(f"  Skip sitemap: {skip_sitemap}")
    logger.info(f"  Only domains: {only_domains}")
    logger.info("")


def log_pipeline_summary(logger, results) -> None:
    """Log pipeline completion summary."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("Pipeline Summary")
    logger.info("=" * 60)
    logger.info(f"  Status: {'SUCCESS' if results['success'] else 'FAILED'}")
    logger.info(f"  Duration: {results['duration_seconds']:.1f}s")
    for stage_name, stage_result in results.get("stages", {}).items():
        status = "ok" if stage_result.get("success") else "failed"
        duration = stage_result.get("duration_seconds", 0)
        logger.info(f"  {status} {stage_name}: {duration:.1f}s")
    if not results["success"]:
        logger.error(f"  Error: {results.get('error', 'Unknown')}")


def execute_stage(tracker, name, results, stage_fn, record_ingestion=False):
    """Run a stage with timing and tracker updates. Returns stage result dict."""
    tracker.start_stage(name)
    start = time.time()
    result = stage_fn()
    result["duration_seconds"] = time.time() - start
    results["stages"][name] = result
    rows = result.get(
        "rows",
        result.get(
            "records",
            result.get(
                "transformed_rows", result.get("sessions", result.get("urls_stored", 0))
            ),
        ),
    )
    if record_ingestion:
        tracker.record_rows(input_rows=result.get("records", 0))
    tracker.complete_stage(rows=rows)
    return result
