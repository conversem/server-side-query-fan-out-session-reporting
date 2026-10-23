#!/usr/bin/env python3
"""
Set up Cloud Monitoring alert policies for a Cloud Run job.

Usage:
    python scripts/setup_cloud_run_alerts.py \\
        --project-id <SOPS gcp.project_id> \\
        --emails ruben@conversem.com

--project-id and --emails are required. There are no project or email defaults.
Client deployments must pass the live GCP project and recipient list.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import GoogleAuthError

from llm_bot_pipeline.infrastructure.cloud_run_alerts import (
    AlertPolicyConflictError,
    AlertTarget,
    setup_cloud_run_job_alerts,
)

DEFAULT_JOB_NAME = "llm-bot-pipeline"
DEFAULT_LOCATION = "europe-west1"
DEFAULT_SCHEDULER_JOB = "daily-llm-bot-pipeline"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Set up Cloud Monitoring email channels and alert policies "
        "for a Cloud Run job."
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID (from SOPS gcp.project_id)",
    )
    parser.add_argument(
        "--job-name",
        default=DEFAULT_JOB_NAME,
        help=f"Cloud Run job name (default: {DEFAULT_JOB_NAME})",
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_LOCATION,
        help=f"Cloud Run job region (default: {DEFAULT_LOCATION})",
    )
    parser.add_argument(
        "--scheduler-job",
        default=DEFAULT_SCHEDULER_JOB,
        help=f"Cloud Scheduler job ID (default: {DEFAULT_SCHEDULER_JOB})",
    )
    parser.add_argument(
        "--emails",
        nargs="+",
        required=True,
        help="Email addresses for alert notifications (one or more)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        target = AlertTarget(
            project_id=args.project_id,
            job_name=args.job_name,
            location=args.region,
            scheduler_job=args.scheduler_job,
        )
        result = setup_cloud_run_job_alerts(target, args.emails)
    except AlertPolicyConflictError as exc:
        print(f"Conflict: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Invalid input: {exc}", file=sys.stderr)
        return 1
    except (GoogleAPIError, GoogleAuthError) as exc:
        print(f"GCP error: {exc}", file=sys.stderr)
        return 1

    print("Notification channels:")
    for name in result.channel_names:
        print(f"  {name}")
    print("Alert policies:")
    for name in result.policy_names:
        print(f"  {name}")
    print()
    print(
        "IMPORTANT: Verify each email channel (alerting-noreply@google.com) "
        "before relying on alerts."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
