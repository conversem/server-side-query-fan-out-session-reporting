"""
Programmatic Cloud Run Job + Cloud Scheduler setup via Python SDK.

Alternative to the shell-based deploy.sh script. Useful when you want
to manage GCP infrastructure from Python (e.g. in a setup script or
notebook) rather than bash.

Requires:
    pip install google-cloud-run google-cloud-scheduler
"""

import logging
from typing import Optional

from ..config.constants import (
    DEFAULT_CLOUD_RUN_CPU,
    DEFAULT_CLOUD_RUN_JOB_NAME,
    DEFAULT_CLOUD_RUN_MEMORY,
    DEFAULT_CLOUD_RUN_TIMEOUT_SECONDS,
    DEFAULT_SCHEDULE,
    DEFAULT_SCHEDULER_NAME,
    DEFAULT_TIMEZONE,
)

logger = logging.getLogger(__name__)


def create_cloud_run_job(
    project_id: str,
    region: str,
    service_account_email: str,
    image: Optional[str] = None,
    job_name: str = DEFAULT_CLOUD_RUN_JOB_NAME,
    memory: str = DEFAULT_CLOUD_RUN_MEMORY,
    cpu: str = DEFAULT_CLOUD_RUN_CPU,
    timeout_seconds: int = DEFAULT_CLOUD_RUN_TIMEOUT_SECONDS,
) -> dict:
    """Create or update a Cloud Run Job for the daily ETL pipeline."""
    try:
        from google.cloud import run_v2
    except ImportError:
        raise ImportError(
            "google-cloud-run is required for programmatic Cloud Run setup. "
            "Install with: pip install google-cloud-run"
        )

    if image is None:
        image = f"gcr.io/{project_id}/llm-bot-pipeline:latest"

    client = run_v2.JobsClient()
    parent = f"projects/{project_id}/locations/{region}"

    job = run_v2.Job(
        template=run_v2.ExecutionTemplate(
            task_count=1,
            template=run_v2.TaskTemplate(
                containers=[
                    run_v2.Container(
                        image=image,
                        command=["python", "scripts/run_daily_etl.py"],
                        args=["--processing-mode", "gcp_bq"],
                        env=[
                            run_v2.EnvVar(name="PROCESSING_MODE", value="gcp_bq"),
                            run_v2.EnvVar(name="USE_SECRET_MANAGER", value="true"),
                            run_v2.EnvVar(name="GCP_PROJECT_ID", value=project_id),
                        ],
                        resources=run_v2.ResourceRequirements(
                            limits={"memory": memory, "cpu": cpu},
                        ),
                    )
                ],
                timeout=f"{timeout_seconds}s",
                service_account=service_account_email,
                max_retries=1,
            ),
        ),
    )

    logger.info("Creating Cloud Run Job '%s' in %s/%s", job_name, project_id, region)

    operation = client.create_job(
        parent=parent,
        job=job,
        job_id=job_name,
    )
    result = operation.result()

    logger.info("Cloud Run Job created: %s", result.name)
    return {"name": result.name, "uri": result.uri}


def create_scheduler_trigger(
    project_id: str,
    region: str,
    service_account_email: str,
    job_name: str = DEFAULT_CLOUD_RUN_JOB_NAME,
    scheduler_name: str = DEFAULT_SCHEDULER_NAME,
    schedule: str = DEFAULT_SCHEDULE,
    timezone: str = DEFAULT_TIMEZONE,
) -> dict:
    """Create a Cloud Scheduler job that triggers the Cloud Run Job daily."""
    try:
        from google.cloud import scheduler_v1
    except ImportError:
        raise ImportError(
            "google-cloud-scheduler is required. "
            "Install with: pip install google-cloud-scheduler"
        )

    client = scheduler_v1.CloudSchedulerClient()
    parent = f"projects/{project_id}/locations/{region}"

    uri = (
        f"https://{region}-run.googleapis.com/apis/run.googleapis.com/v1/"
        f"namespaces/{project_id}/jobs/{job_name}:run"
    )

    job = scheduler_v1.Job(
        name=f"{parent}/jobs/{scheduler_name}",
        schedule=schedule,
        time_zone=timezone,
        http_target=scheduler_v1.HttpTarget(
            uri=uri,
            http_method=scheduler_v1.HttpMethod.POST,
            oauth_token=scheduler_v1.OAuthToken(
                service_account_email=service_account_email,
                scope="https://www.googleapis.com/auth/cloud-platform",
            ),
        ),
        retry_config=scheduler_v1.RetryConfig(
            retry_count=2,
        ),
    )

    logger.info("Creating Cloud Scheduler job '%s'", scheduler_name)

    result = client.create_job(parent=parent, job=job)
    logger.info("Scheduler job created: %s", result.name)
    return {"name": result.name, "schedule": result.schedule}


def setup_gcp_bq_infrastructure(
    project_id: str,
    region: str,
    service_account_email: str,
    image: Optional[str] = None,
) -> dict:
    """One-call setup for the complete gcp_bq deployment."""
    logger.info("Setting up gcp_bq infrastructure for %s in %s", project_id, region)

    job_info = create_cloud_run_job(
        project_id=project_id,
        region=region,
        service_account_email=service_account_email,
        image=image,
    )

    scheduler_info = create_scheduler_trigger(
        project_id=project_id,
        region=region,
        service_account_email=service_account_email,
    )

    return {"cloud_run_job": job_info, "scheduler": scheduler_info}
