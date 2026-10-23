"""Idempotent GCP Storage Transfer Service job setup for an AWS S3 log source.

One transfer job per (client, source_type, prefix). The job:
  - reads from S3 using awsS3DataSource.roleArn (G14 federated identity)
  - writes to the per-client GCS staging bucket
  - runs daily at the configured UTC hour
  - overwrites objects only when content differs (incremental sync)
  - never deletes source objects

The script is safe to re-run. Existing jobs (matched by description) are
updated; missing jobs are created.

WARNING: G14 verification gate. The first time this script runs in a new
environment, exercise it against a sandbox AWS account and confirm STS
performs the cross-cloud AssumeRoleWithWebIdentity successfully. If
role_arn turns out to be unavailable in the installed client version,
fall back to a short-lived-access-key path (out of scope here; see the
spec's Round-2 deferrals).
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from google.cloud import storage_transfer

logger = logging.getLogger(__name__)


@dataclass
class TransferJobSpec:
    project_id: str
    client_name: str
    aws_bucket: str
    aws_prefix: str
    reader_role_arn: str
    gcs_staging_bucket: str
    gcs_staging_prefix: str
    schedule_hour_utc: int = 3  # D6: spec §1 locked-in STS at 03:00 UTC (ETL at 04:00)


def _job_description(spec: TransferJobSpec) -> str:
    safe_prefix = spec.aws_prefix.rstrip("/").replace("/", "-")
    return f"aws-logs-{spec.client_name}-{safe_prefix}"


def build_transfer_job_request(spec: TransferJobSpec) -> dict:
    """Build the create_transfer_job request payload (dict form).

    Returned as a plain dict so it is trivially diffable in tests; the STS
    client accepts the dict shape directly via its message factories.
    """
    today = datetime.now(timezone.utc).date()
    return {
        "transfer_job": {
            "description": _job_description(spec),
            "project_id": spec.project_id,
            "status": "ENABLED",
            "transfer_spec": {
                "aws_s3_data_source": {
                    "bucket_name": spec.aws_bucket,
                    "path": spec.aws_prefix,
                    "role_arn": spec.reader_role_arn,
                },
                "gcs_data_sink": {
                    "bucket_name": spec.gcs_staging_bucket,
                    "path": spec.gcs_staging_prefix,
                },
                "transfer_options": {
                    "overwrite_when": "DIFFERENT",
                    "delete_objects_from_source_after_transfer": False,
                },
            },
            "schedule": {
                "schedule_start_date": {
                    "year": today.year,
                    "month": today.month,
                    "day": today.day,
                },
                "start_time_of_day": {
                    "hours": spec.schedule_hour_utc,
                    "minutes": 0,
                    "seconds": 0,
                    "nanos": 0,
                },
            },
        }
    }


def upsert_transfer_job(client, spec: TransferJobSpec) -> None:
    """Create or update the transfer job for `spec`. Idempotent."""
    request = build_transfer_job_request(spec)
    description = request["transfer_job"]["description"]

    existing = None
    for job in client.list_transfer_jobs(
        request={"filter": f'{{"project_id":"{spec.project_id}"}}'}
    ):
        if getattr(job, "description", None) == description:
            existing = job
            break

    if existing is None:
        result = client.create_transfer_job(request=request)
        logger.info(
            "Created transfer job: %s",
            result.name if hasattr(result, "name") else result,
        )
    else:
        client.update_transfer_job(
            request={
                "job_name": existing.name,
                "project_id": spec.project_id,
                "transfer_job": request["transfer_job"],
                "update_transfer_job_field_mask": "transferSpec,schedule,status,description",
            }
        )
        logger.info("Updated transfer job: %s", existing.name)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--client-name", required=True)
    parser.add_argument("--aws-bucket", required=True)
    parser.add_argument("--aws-prefix", required=True)
    parser.add_argument("--reader-role-arn", required=True)
    parser.add_argument("--gcs-staging-bucket", required=True)
    parser.add_argument("--gcs-staging-prefix", required=True)
    parser.add_argument("--schedule-hour-utc", type=int, default=3)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spec = TransferJobSpec(
        project_id=args.project_id,
        client_name=args.client_name,
        aws_bucket=args.aws_bucket,
        aws_prefix=args.aws_prefix,
        reader_role_arn=args.reader_role_arn,
        gcs_staging_bucket=args.gcs_staging_bucket,
        gcs_staging_prefix=args.gcs_staging_prefix,
        schedule_hour_utc=args.schedule_hour_utc,
    )

    client = storage_transfer.StorageTransferServiceClient()
    upsert_transfer_job(client=client, spec=spec)
    print(f"OK: STS job for {args.client_name} / {args.aws_prefix} is ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
