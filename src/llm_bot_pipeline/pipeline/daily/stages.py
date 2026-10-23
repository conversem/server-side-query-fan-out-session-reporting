"""Operational stages for the daily BigQuery ETL runner."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from ...aws.s3_log_source import pull_shared_aws_logs
from ...config import OPTIMAL_WINDOW_MS
from ...config.constants import DATASET_REPORT, TABLE_CLEAN_BOT_REQUESTS
from ...config.settings import DomainConfig, Settings
from ...pipeline.orchestrator import ETLPipeline
from ...reporting import ReportingAggregator, SessionAggregator
from ...storage import get_backend
from .ingestion_groups import group_domains_for_ingestion
from .runtime import run_sitemap_stage_bq

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def ingest_domain(
    pull_fn,
    backend,
    start_date: date,
    end_date: date,
    settings,
    domain_config,
    logger: logging.Logger | None = None,
) -> int:
    """Ingest logs for a single domain into the backend."""
    logger = logger or logging.getLogger(__name__)
    total_records = 0
    current = start_date
    days_processed = 0
    total_days = (end_date - start_date).days + 1
    label = f" [{domain_config.domain}]"

    while current <= end_date:
        days_processed += 1
        progress = (days_processed / total_days) * 100
        logger.info(
            f"[Ingestion]{label} [{progress:5.1f}%] Processing {current} "
            f"({days_processed}/{total_days})"
        )

        try:
            records = pull_fn(
                start_date=current,
                end_date=current,
                domain_config=domain_config,
                settings=settings,
            )

            if records:
                rows_inserted = backend.insert_raw_records(records)
                total_records += rows_inserted
                logger.info(
                    f"[Ingestion]{label} Inserted {rows_inserted:,} records for {current}"
                )
            else:
                logger.warning(f"[Ingestion]{label} No records found for {current}")

        except Exception as e:
            logger.error(f"[Ingestion]{label} Failed for {current}: {e}")
            raise

        current += timedelta(days=1)

    return total_records


def ingest_shared_aws_domains(
    backend,
    start_date: date,
    end_date: date,
    settings: Settings,
    domain_configs: list[DomainConfig],
    logger: logging.Logger | None = None,
) -> int:
    """Ingest several AWS domains from one download of their shared GCS prefix."""
    logger = logger or logging.getLogger(__name__)
    names = ", ".join(domain_config.domain for domain_config in domain_configs)
    logger.info("[Ingestion] Shared AWS scan for %s", names)
    total_records = 0
    current = start_date
    days_processed = 0
    total_days = (end_date - start_date).days + 1

    while current <= end_date:
        days_processed += 1
        progress = (days_processed / total_days) * 100
        logger.info(
            f"[Ingestion] [{names}] [{progress:5.1f}%] Processing {current} "
            f"({days_processed}/{total_days})"
        )

        try:
            buckets: dict[str, list[dict]] = {
                domain_config.domain: [] for domain_config in domain_configs
            }
            for row in pull_shared_aws_logs(
                start_date=current,
                end_date=current,
                domain_configs=domain_configs,
                settings=settings,
            ):
                buckets[row["domain"]].append(row)

            for domain_config in domain_configs:
                rows = buckets[domain_config.domain]
                label = f" [{domain_config.domain}]"
                if rows:
                    rows_inserted = backend.insert_raw_records(rows)
                    total_records += rows_inserted
                    logger.info(
                        f"[Ingestion]{label} Inserted {rows_inserted:,} "
                        f"records for {current}"
                    )
                else:
                    logger.warning(f"[Ingestion]{label} No records found for {current}")
        except Exception as e:
            logger.error(f"[Ingestion] Shared AWS scan failed for {current}: {e}")
            raise

        current += timedelta(days=1)

    return total_records


def resolve_creds(config: dict) -> tuple[str, str]:
    """Extract project_id and resolved credentials path from config."""
    project_id = config.get("gcp", {}).get("project_id")
    creds_path = config.get("gcp", {}).get("service_account_key_path")
    if creds_path and not Path(creds_path).is_absolute():
        if not creds_path.startswith("credentials/"):
            creds_path = str(PROJECT_ROOT / "credentials" / creds_path)
        else:
            creds_path = str(PROJECT_ROOT / creds_path)
    return project_id, creds_path


def run_ingestion(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
    only_domains: list[str] | None = None,
) -> dict:
    """Pull logs per configured domain into raw storage."""
    logger = logger or logging.getLogger(__name__)
    logger.info(f"[Ingestion] Starting for {start_date} to {end_date}")

    if dry_run:
        logger.info("[Ingestion] DRY RUN - skipping actual ingestion")
        return {"success": True, "records": 0, "dry_run": True}

    from ...config.settings import get_settings
    from ...pipeline.ingestion_dispatch import dispatch

    settings = get_settings()
    use_multi_domain = bool(settings.domains)

    if not use_multi_domain:
        if not settings.cloudflare_api_token or not settings.cloudflare_zone_id:
            raise RuntimeError(
                "Cloudflare credentials not configured. "
                "Provide cloudflare.api_token + cloudflare.zone_id, "
                "or use the domains[] config for multi-domain."
            )

    project_id, creds_path = resolve_creds(config)
    backend = get_backend(
        backend_type="bigquery",
        project_id=project_id,
        credentials_path=creds_path,
    )

    try:
        backend.initialize()
        total_records = 0

        if use_multi_domain:
            domains = settings.domains
            if only_domains is not None:
                domains = [dc for dc in settings.domains if dc.domain in only_domains]
            logger.info(f"[Ingestion] Multi-domain mode: {len(domains)} domain(s)")
            for group in group_domains_for_ingestion(domains):
                for dc in group:
                    logger.info(
                        f"[Ingestion] --- Domain: {dc.domain} "
                        f"(provider={dc.provider}) ---"
                    )
                if len(group) == 1:
                    dc = group[0]
                    pull_fn = dispatch(dc)
                    total_records += ingest_domain(
                        pull_fn=pull_fn,
                        backend=backend,
                        start_date=start_date,
                        end_date=end_date,
                        settings=settings,
                        domain_config=dc,
                        logger=logger,
                    )
                else:
                    total_records += ingest_shared_aws_domains(
                        backend=backend,
                        start_date=start_date,
                        end_date=end_date,
                        settings=settings,
                        domain_configs=group,
                        logger=logger,
                    )
        else:
            legacy_dc = DomainConfig(
                domain=settings.cloudflare_zone_id,
                zone_id=settings.cloudflare_zone_id,
                provider="cloudflare",
            )
            pull_fn = dispatch(legacy_dc)
            total_records = ingest_domain(
                pull_fn=pull_fn,
                backend=backend,
                start_date=start_date,
                end_date=end_date,
                settings=settings,
                domain_config=legacy_dc,
                logger=logger,
            )

        logger.info(f"[Ingestion] Complete: {total_records:,} records ingested")
        return {"success": True, "records": total_records}

    finally:
        backend.close()


def run_etl_pipeline(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict:
    """Run the BigQuery ETL pipeline from raw to clean tables."""
    logger = logger or logging.getLogger(__name__)
    logger.info(f"[ETL] Starting pipeline for {start_date} to {end_date}")

    project_id, creds_path = resolve_creds(config)

    if dry_run:
        logger.info("[ETL] DRY RUN - skipping actual ETL")
        return {"success": True, "rows_written": 0, "dry_run": True}

    pipeline = ETLPipeline(
        project_id=project_id,
        credentials_path=creds_path,
    )

    try:
        result = pipeline.run(
            start_date=start_date,
            end_date=end_date,
            mode="full",
        )

        logger.info(
            f"[ETL] Complete: {result.transformed_rows:,} rows transformed, "
            f"{result.duplicates_removed:,} duplicates removed"
        )

        return {
            "success": result.success,
            "raw_rows": result.raw_rows,
            "transformed_rows": result.transformed_rows,
            "duplicates_removed": result.duplicates_removed,
            "error": "; ".join(result.errors) if result.errors else None,
        }

    except Exception as e:
        logger.error(f"[ETL] Pipeline failed: {e}")
        return {"success": False, "error": str(e)}


def run_aggregations(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
    track_freshness: bool = True,
) -> dict:
    """Run aggregations stage: clean to daily_summary and url_performance."""
    logger = logger or logging.getLogger(__name__)
    logger.info(f"[Aggregations] Starting for {start_date} to {end_date}")

    if dry_run:
        logger.info("[Aggregations] DRY RUN - skipping")
        return {"success": True, "rows": 0, "dry_run": True}

    project_id, creds_path = resolve_creds(config)

    try:
        aggregator = ReportingAggregator(
            project_id=project_id,
            credentials_path=creds_path,
        )

        total_rows = 0
        logger.info("[Aggregations] Creating daily_summary...")
        daily_result = aggregator.aggregate_daily_summary(start_date, end_date)
        if daily_result.success:
            if track_freshness:
                aggregator.log_freshness(daily_result)
            total_rows += daily_result.rows_inserted
            logger.info(
                f"[Aggregations] daily_summary: {daily_result.rows_inserted:,} rows"
            )
        else:
            logger.error(f"[Aggregations] daily_summary failed: {daily_result.error}")
            aggregator.close()
            return {"success": False, "error": f"daily_summary: {daily_result.error}"}

        logger.info("[Aggregations] Creating url_performance...")
        url_result = aggregator.aggregate_url_performance(start_date, end_date)
        if url_result.success:
            if track_freshness:
                aggregator.log_freshness(url_result)
            total_rows += url_result.rows_inserted
            logger.info(
                f"[Aggregations] url_performance: {url_result.rows_inserted:,} rows"
            )
        else:
            logger.error(f"[Aggregations] url_performance failed: {url_result.error}")
            aggregator.close()
            return {
                "success": False,
                "error": f"url_performance: {url_result.error}",
            }

        aggregator.close()
        logger.info(f"[Aggregations] Complete: {total_rows:,} rows")
        return {"success": True, "rows": total_rows}

    except Exception as e:
        logger.error(f"[Aggregations] Failed: {e}")
        return {"success": False, "error": str(e)}


def run_sessions(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict:
    """Run sessions stage: clean to query_fanout_sessions."""
    import pandas as pd

    logger = logger or logging.getLogger(__name__)
    logger.info(f"[Sessions] Starting for {start_date} to {end_date}")

    if dry_run:
        logger.info("[Sessions] DRY RUN - skipping")
        return {"success": True, "sessions": 0, "dry_run": True}

    project_id, creds_path = resolve_creds(config)
    backend = get_backend(
        "bigquery",
        project_id=project_id,
        credentials_path=creds_path,
    )

    try:
        backend.initialize()
        logger.info("[Sessions] Fetching user-request traffic...")
        table_name = f"`{project_id}.{DATASET_REPORT}.{TABLE_CLEAN_BOT_REQUESTS}`"
        query = f"""
            SELECT
                request_timestamp AS datetime,
                url_path AS url,
                bot_provider,
                bot_name,
                domain
            FROM {table_name}
            WHERE bot_category = 'user_request'
              AND request_date >= @start_date
              AND request_date <= @end_date
            ORDER BY request_timestamp
        """

        rows = backend.query(
            query,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
        logger.info(f"[Sessions] Found {len(rows):,} user-request records")

        if not rows:
            logger.info("[Sessions] No user-request data to process")
            return {"success": True, "sessions": 0, "rows_processed": 0}

        df = pd.DataFrame(rows)
        df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")

        with SessionAggregator(backend=backend) as aggregator:
            for day in pd.date_range(start_date, end_date):
                aggregator.delete_sessions(session_date=day.strftime("%Y-%m-%d"))

            agg_result = aggregator.create_sessions_from_dataframe(
                df=df,
                window_ms=OPTIMAL_WINDOW_MS,
                timestamp_col="datetime",
                url_col="url",
                group_by="bot_provider",
                bot_name_col="bot_name",
            )

        if agg_result.success:
            logger.info(
                f"[Sessions] Created {agg_result.sessions_created:,} sessions "
                f"(high={agg_result.high_confidence_count}, "
                f"medium={agg_result.medium_confidence_count}, "
                f"low={agg_result.low_confidence_count})"
            )
        else:
            logger.error(f"[Sessions] Failed: {agg_result.error}")

        return {
            "success": agg_result.success,
            "sessions": agg_result.sessions_created,
            "rows_processed": len(df),
            "error": agg_result.error,
        }

    except Exception as e:
        logger.error(f"[Sessions] Failed: {e}")
        return {"success": False, "error": str(e)}

    finally:
        backend.close()


def run_url_details(
    config: dict,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict:
    """Populate session_url_details table for dashboarding."""
    logger = logger or logging.getLogger(__name__)
    logger.info(f"[URL Details] Populating for {start_date} to {end_date}")

    if dry_run:
        logger.info("[URL Details] DRY RUN - skipping")
        return {"success": True, "rows": 0, "dry_run": True}

    project_id, creds_path = resolve_creds(config)
    backend = get_backend(
        backend_type="bigquery",
        project_id=project_id,
        credentials_path=creds_path,
    )

    aggregator = None
    try:
        aggregator = SessionAggregator(backend=backend)
        aggregator.initialize()
        rows = aggregator.populate_url_details(
            start_date=str(start_date),
            end_date=str(end_date),
            delete_existing=True,
        )

        logger.info(f"[URL Details] Complete: {rows:,} rows populated")
        return {"success": True, "rows": rows}

    except Exception as e:
        logger.error(f"[URL Details] Failed: {e}")
        return {"success": False, "error": str(e)}

    finally:
        if aggregator is not None:
            aggregator.close()
        backend.close()


def run_sitemap_stage(
    config: dict,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict:
    """Run sitemap fetch and aggregation using BigQuery backend."""
    return run_sitemap_stage_bq(config, resolve_creds, dry_run, logger)
