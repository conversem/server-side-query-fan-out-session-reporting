"""
Cloudflare Logpush job management for LLM bot traffic.

Provides CRUD operations for Logpush jobs pushing to BigQuery.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from cloudflare import Cloudflare

logger = logging.getLogger(__name__)

from ..config.constants import OUTPUT_FIELDS, TABLE_RAW_BOT_REQUESTS
from ..config.settings import Settings
from .filters import get_filter_json


@dataclass
class LogpushJobResult:
    """Result of a Logpush job operation."""

    success: bool
    job_id: Optional[int] = None
    message: str = ""
    error: Optional[str] = None


def get_cloudflare_client(settings: Settings) -> Cloudflare:
    """
    Create authenticated Cloudflare client.

    Args:
        settings: Application settings with Cloudflare credentials

    Returns:
        Authenticated Cloudflare client
    """
    return Cloudflare(api_token=settings.cloudflare_api_token)


def build_bigquery_destination(settings: Settings) -> str:
    """
    Build BigQuery destination configuration string.

    Format: bigquery://<project_id>/<dataset_id>/<table_id>

    Args:
        settings: Application settings with GCP/BigQuery config

    Returns:
        BigQuery destination URI for Logpush
    """
    project_id = settings.gcp_project_id
    dataset_id = settings.dataset_raw
    table_id = TABLE_RAW_BOT_REQUESTS

    return f"bigquery://{project_id}/{dataset_id}/{table_id}"


def build_logpull_options() -> str:
    """
    Build logpull options string with output fields and format settings.

    Returns:
        Logpull options string for Logpush API
    """
    fields = ",".join(OUTPUT_FIELDS)
    return f"fields={fields}&timestamps=rfc3339"


def create_logpush_job(settings: Settings) -> LogpushJobResult:
    """
    Create a new Cloudflare Logpush job for LLM bot traffic.

    Creates job that:
    - No pre-filter applied — all HTTP requests are pushed; LLM bot
      classification is done post-ingestion via user-agent pattern matching
    - Extracts specified fields from http_requests dataset
    - Pushes to BigQuery in ndjson format with rfc3339 timestamps

    Args:
        settings: Application settings with Cloudflare and GCP config

    Returns:
        LogpushJobResult with success status and job details
    """
    try:
        client = get_cloudflare_client(settings)

        existing = find_job_by_name(settings, settings.logpush_job_name)
        if existing.job_id:
            return LogpushJobResult(
                success=False,
                job_id=existing.job_id,
                message=f"Job '{settings.logpush_job_name}' already exists",
                error="Job already exists. Use update or delete first.",
            )

        destination_conf = build_bigquery_destination(settings)
        logpull_options = build_logpull_options()
        filter_json = get_filter_json()

        job = client.logpush.jobs.create(
            zone_id=settings.cloudflare_zone_id,
            name=settings.logpush_job_name,
            destination_conf=destination_conf,
            dataset="http_requests",
            enabled=settings.logpush_enabled,
            logpull_options=logpull_options,
            filter=filter_json,
            output_options={
                "output_type": "ndjson",
                "timestamp_format": "rfc3339",
            },
        )

        return LogpushJobResult(
            success=True,
            job_id=job.id,
            message=f"Successfully created Logpush job: {settings.logpush_job_name}",
        )

    except Exception as e:
        return LogpushJobResult(
            success=False, message="Failed to create Logpush job", error=str(e)
        )


def find_job_by_name(settings: Settings, job_name: str) -> LogpushJobResult:
    """
    Find a Logpush job by name.

    Args:
        settings: Application settings with Cloudflare config
        job_name: Name of the job to find

    Returns:
        LogpushJobResult with job_id if found
    """
    try:
        client = get_cloudflare_client(settings)

        for job in client.logpush.jobs.list(zone_id=settings.cloudflare_zone_id):
            if job.name == job_name:
                return LogpushJobResult(
                    success=True,
                    job_id=job.id,
                    message=f"Found job: {job_name}",
                )

        return LogpushJobResult(success=True, message=f"Job not found: {job_name}")

    except Exception as e:
        return LogpushJobResult(
            success=False, message="Failed to list Logpush jobs", error=str(e)
        )


def list_logpush_jobs(settings: Settings) -> list[dict]:
    """
    List all Logpush jobs for the zone.

    Args:
        settings: Application settings with Cloudflare config

    Returns:
        List of job dictionaries with id, name, enabled, dataset, destination
    """
    try:
        client = get_cloudflare_client(settings)
        jobs = []

        for job in client.logpush.jobs.list(zone_id=settings.cloudflare_zone_id):
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "enabled": job.enabled,
                    "dataset": getattr(job, "dataset", None),
                    "destination": getattr(job, "destination_conf", None),
                }
            )

        return jobs

    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return []


def update_logpush_job(
    settings: Settings, job_id: int, enabled: Optional[bool] = None
) -> LogpushJobResult:
    """
    Update an existing Logpush job.

    Args:
        settings: Application settings with Cloudflare config
        job_id: ID of the job to update
        enabled: Optional new enabled state

    Returns:
        LogpushJobResult with success status
    """
    try:
        client = get_cloudflare_client(settings)

        update_params = {}
        if enabled is not None:
            update_params["enabled"] = enabled

        if not update_params:
            return LogpushJobResult(
                success=False, job_id=job_id, error="No update parameters provided"
            )

        client.logpush.jobs.update(
            job_id=job_id, zone_id=settings.cloudflare_zone_id, **update_params
        )

        return LogpushJobResult(
            success=True, job_id=job_id, message=f"Successfully updated job {job_id}"
        )

    except Exception as e:
        return LogpushJobResult(
            success=False,
            job_id=job_id,
            message="Failed to update Logpush job",
            error=str(e),
        )


def delete_logpush_job(settings: Settings, job_id: int) -> LogpushJobResult:
    """
    Delete a Logpush job.

    Args:
        settings: Application settings with Cloudflare config
        job_id: ID of the job to delete

    Returns:
        LogpushJobResult with success status
    """
    try:
        client = get_cloudflare_client(settings)

        client.logpush.jobs.delete(job_id=job_id, zone_id=settings.cloudflare_zone_id)

        return LogpushJobResult(
            success=True, job_id=job_id, message=f"Successfully deleted job {job_id}"
        )

    except Exception as e:
        return LogpushJobResult(
            success=False,
            job_id=job_id,
            message="Failed to delete Logpush job",
            error=str(e),
        )


def enable_logpush_job(settings: Settings, job_id: int) -> LogpushJobResult:
    """
    Enable a Logpush job.

    Args:
        settings: Application settings with Cloudflare config
        job_id: ID of the job to enable

    Returns:
        LogpushJobResult with success status
    """
    return update_logpush_job(settings, job_id, enabled=True)


def disable_logpush_job(settings: Settings, job_id: int) -> LogpushJobResult:
    """
    Disable a Logpush job.

    Args:
        settings: Application settings with Cloudflare config
        job_id: ID of the job to disable

    Returns:
        LogpushJobResult with success status
    """
    return update_logpush_job(settings, job_id, enabled=False)
